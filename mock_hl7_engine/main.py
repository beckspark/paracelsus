"""Mock HL7v2 MLLP engine.

Two servers in one process:
- MLLP TCP server on port 2575 (asyncio, started via FastAPI lifespan)
- HTTP REST API on port 8090 (FastAPI/uvicorn)

Receives HL7v2 pipe-delimited messages over MLLP, parses them, stores in memory.
Exposes parsed data as JSON for tap-rest-api-msdk to consume.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI

# ---------------------------------------------------------------------------
# MLLP framing constants
# ---------------------------------------------------------------------------
MLLP_START = b"\x0b"
MLLP_END = b"\x1c\x0d"

# ---------------------------------------------------------------------------
# In-memory stores (asyncio single-threaded — no locking needed)
# ---------------------------------------------------------------------------
ADMISSIONS: list[dict] = []
DISCHARGES: list[dict] = []
LAB_RESULTS: list[dict] = []


# ---------------------------------------------------------------------------
# HL7v2 parsing
# ---------------------------------------------------------------------------


def _parse_hl7_segments(raw_message: str) -> dict[str, list[list[str]]]:
    """Parse raw HL7v2 message into segment name → list of field lists."""
    segments: dict[str, list[list[str]]] = {}
    for line in raw_message.strip().split("\n"):
        fields = line.split("|")
        seg_name = fields[0]
        if seg_name not in segments:
            segments[seg_name] = []
        segments[seg_name].append(fields)
    return segments


def _parse_admission(raw: str) -> dict:
    segs = _parse_hl7_segments(raw)
    msh = segs["MSH"][0]
    pid = segs["PID"][0]
    pv1 = segs["PV1"][0]
    name_parts = pid[5].split("^")
    attending = pv1[7].split("^") if len(pv1) > 7 else []
    return {
        "message_id": msh[9],
        "patient_mrn": pid[3],
        "patient_last_name": name_parts[0] if name_parts else "",
        "patient_first_name": name_parts[1] if len(name_parts) > 1 else "",
        "patient_dob": pid[7],
        "patient_gender": pid[8],
        "attending_provider_npi": attending[0] if attending else "",
        "patient_class": pv1[2],
        "admit_datetime": pv1[44] if len(pv1) > 44 else "",
        "visit_number": pv1[19] if len(pv1) > 19 else "",
        "sending_facility": msh[3],
        "message_datetime": msh[6],
    }


def _parse_discharge(raw: str) -> dict:
    segs = _parse_hl7_segments(raw)
    msh = segs["MSH"][0]
    pid = segs["PID"][0]
    pv1 = segs["PV1"][0]
    name_parts = pid[5].split("^")
    attending = pv1[7].split("^") if len(pv1) > 7 else []
    return {
        "message_id": msh[9],
        "patient_mrn": pid[3],
        "patient_last_name": name_parts[0] if name_parts else "",
        "patient_first_name": name_parts[1] if len(name_parts) > 1 else "",
        "attending_provider_npi": attending[0] if attending else "",
        "patient_class": pv1[2],
        "admit_datetime": pv1[44] if len(pv1) > 44 else "",
        "discharge_datetime": pv1[45] if len(pv1) > 45 else "",
        "visit_number": pv1[19] if len(pv1) > 19 else "",
        "sending_facility": msh[3],
        "message_datetime": msh[6],
    }


def _parse_lab_result(raw: str) -> list[dict]:
    """Parse ORU^R01 into one dict per OBX segment."""
    segs = _parse_hl7_segments(raw)
    msh = segs["MSH"][0]
    pid = segs["PID"][0]
    obr = segs["OBR"][0]
    name_parts = pid[5].split("^")
    rows = []
    for obx in segs.get("OBX", []):
        test_parts = obx[3].split("^")
        rows.append(
            {
                "message_id": msh[9],
                "patient_mrn": pid[3],
                "patient_last_name": name_parts[0] if name_parts else "",
                "patient_first_name": name_parts[1] if len(name_parts) > 1 else "",
                "ordering_provider_npi": obr[16] if len(obr) > 16 else "",
                "order_number": obr[2],
                "test_code": test_parts[0] if test_parts else "",
                "test_name": test_parts[1] if len(test_parts) > 1 else "",
                "result_value": obx[5],
                "result_units": obx[6],
                "reference_range": obx[7],
                "abnormal_flag": obx[8],
                "observation_datetime": msh[6],
                "sending_facility": msh[3],
                "message_datetime": msh[6],
            }
        )
    return rows


# ---------------------------------------------------------------------------
# MLLP server
# ---------------------------------------------------------------------------


def _get_msg_id(raw: str) -> str:
    """Extract message control ID from MSH segment (field 9)."""
    for line in raw.split("\n"):
        if line.startswith("MSH|"):
            fields = line.split("|")
            return fields[9] if len(fields) > 9 else ""
    return ""


def _get_msg_type(raw: str) -> str:
    """Extract message type from MSH segment (field 8)."""
    for line in raw.split("\n"):
        if line.startswith("MSH|"):
            fields = line.split("|")
            return fields[8] if len(fields) > 8 else ""
    return ""


def _build_ack(msg_id: str) -> bytes:
    """Build an MLLP-framed ACK message."""
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    ack = f"MSH|^~\\&|WAREHOUSE|ANALYTICS|SENDER|HOSPITAL|{now}||ACK|ACK{msg_id}|P|2.3\nMSA|AA|{msg_id}"
    return MLLP_START + ack.encode() + MLLP_END


def _route_message(raw: str) -> None:
    """Parse and route an HL7 message to the appropriate in-memory store."""
    msg_type = _get_msg_type(raw)
    if msg_type == "ADT^A01":
        ADMISSIONS.append(_parse_admission(raw))
    elif msg_type == "ADT^A03":
        DISCHARGES.append(_parse_discharge(raw))
    elif msg_type == "ORU^R01":
        LAB_RESULTS.extend(_parse_lab_result(raw))
    else:
        print(f"MLLP: Unknown message type: {msg_type!r}")


async def _handle_mllp_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Handle a single MLLP client connection, processing messages until disconnect."""
    try:
        buf = b""
        while True:
            data = await reader.read(4096)
            if not data:
                break
            buf += data
            while MLLP_START in buf and MLLP_END in buf:
                start = buf.find(MLLP_START)
                end = buf.find(MLLP_END, start)
                if end == -1:
                    break
                raw_bytes = buf[start + 1 : end]
                buf = buf[end + 2 :]
                raw_msg = raw_bytes.decode(errors="replace")
                msg_id = _get_msg_id(raw_msg)
                _route_message(raw_msg)
                writer.write(_build_ack(msg_id))
                await writer.drain()
    except (ConnectionResetError, asyncio.IncompleteReadError):
        pass
    finally:
        writer.close()
    print(
        f"MLLP: Session complete — "
        f"{len(ADMISSIONS)} admissions, {len(DISCHARGES)} discharges, {len(LAB_RESULTS)} lab results"
    )


async def _run_mllp_server() -> None:
    """Start the MLLP TCP server and serve forever."""
    server = await asyncio.start_server(_handle_mllp_client, "0.0.0.0", 2575)
    print("MLLP server listening on 0.0.0.0:2575")
    async with server:
        await server.serve_forever()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_run_mllp_server())
    yield
    task.cancel()


app = FastAPI(title="Mock HL7v2 Engine", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "counts": {
            "admissions": len(ADMISSIONS),
            "discharges": len(DISCHARGES),
            "lab_results": len(LAB_RESULTS),
        },
    }


@app.get("/hl7/admissions")
async def get_admissions():
    return {"records": ADMISSIONS, "total": len(ADMISSIONS)}


@app.get("/hl7/discharges")
async def get_discharges():
    return {"records": DISCHARGES, "total": len(DISCHARGES)}


@app.get("/hl7/lab_results")
async def get_lab_results():
    return {"records": LAB_RESULTS, "total": len(LAB_RESULTS)}
