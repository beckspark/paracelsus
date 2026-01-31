#!/bin/bash
set -e

# Install self-signed cert to system trust store if it exists
if [ -f /certs/cert.pem ]; then
    echo "Installing mock HubSpot certificate to system trust store..."
    cp /certs/cert.pem /usr/local/share/ca-certificates/mock-hubspot.crt
    update-ca-certificates 2>/dev/null || true

    # Also append to certifi CA bundle used by Python requests (system-wide)
    CERTIFI_CA=$(python3 -c "import certifi; print(certifi.where())" 2>/dev/null || echo "")
    if [ -n "$CERTIFI_CA" ] && [ -f "$CERTIFI_CA" ]; then
        echo "Appending cert to system certifi CA bundle: $CERTIFI_CA"
        cat /certs/cert.pem >> "$CERTIFI_CA"
    fi

    # Also patch tap-hubspot's virtualenv certifi if it exists
    # Use glob to handle any Python version (3.11, 3.12, etc.)
    TAP_CERTIFI=$(find /project/.meltano/extractors/tap-hubspot/venv/lib -name "cacert.pem" -path "*/certifi/*" 2>/dev/null | head -1)
    if [ -n "$TAP_CERTIFI" ] && [ -f "$TAP_CERTIFI" ]; then
        echo "Appending cert to tap-hubspot certifi CA bundle: $TAP_CERTIFI"
        cat /certs/cert.pem >> "$TAP_CERTIFI"
    fi

    echo "Certificate installed."
fi

# Execute the command passed to docker
exec "$@"
