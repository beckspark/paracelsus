-- OLTP Schema for Provider Supervision Application
-- This represents the source system that would typically run on RDS

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- States: State licensing info
CREATE TABLE states (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code CHAR(2) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    supervision_requirements TEXT,
    review_frequency_days INT DEFAULT 30,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Physicians: Supervising physicians
CREATE TABLE physicians (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    npi VARCHAR(10) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    specialty VARCHAR(100),
    state_license_id UUID REFERENCES states(id),
    email VARCHAR(255),
    phone VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Providers: NPs/PAs being supervised
CREATE TABLE providers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    npi VARCHAR(10) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    provider_type VARCHAR(10) NOT NULL CHECK (provider_type IN ('NP', 'PA')),
    supervising_physician_id UUID REFERENCES physicians(id),
    state_id UUID REFERENCES states(id),
    email VARCHAR(255),
    phone VARCHAR(50),
    hire_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Cases: Patient cases assigned to providers
CREATE TABLE cases (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider_id UUID NOT NULL REFERENCES providers(id),
    patient_mrn VARCHAR(50) NOT NULL,
    case_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'open' CHECK (status IN ('open', 'closed', 'pending_review')),
    priority VARCHAR(10) DEFAULT 'normal' CHECK (priority IN ('low', 'normal', 'high', 'urgent')),
    created_at TIMESTAMP DEFAULT NOW(),
    closed_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Case Reviews: Review records by supervising physicians
CREATE TABLE case_reviews (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    case_id UUID NOT NULL REFERENCES cases(id),
    physician_id UUID NOT NULL REFERENCES physicians(id),
    review_date DATE NOT NULL,
    review_status VARCHAR(20) NOT NULL CHECK (review_status IN ('pending', 'completed', 'overdue')),
    notes TEXT,
    due_date DATE NOT NULL,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX idx_physicians_state ON physicians(state_license_id);
CREATE INDEX idx_physicians_npi ON physicians(npi);
CREATE INDEX idx_providers_physician ON providers(supervising_physician_id);
CREATE INDEX idx_providers_state ON providers(state_id);
CREATE INDEX idx_providers_npi ON providers(npi);
CREATE INDEX idx_cases_provider ON cases(provider_id);
CREATE INDEX idx_cases_status ON cases(status);
CREATE INDEX idx_case_reviews_case ON case_reviews(case_id);
CREATE INDEX idx_case_reviews_physician ON case_reviews(physician_id);
CREATE INDEX idx_case_reviews_status ON case_reviews(review_status);
CREATE INDEX idx_case_reviews_due_date ON case_reviews(due_date);

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_states_updated_at BEFORE UPDATE ON states
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_physicians_updated_at BEFORE UPDATE ON physicians
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_providers_updated_at BEFORE UPDATE ON providers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_cases_updated_at BEFORE UPDATE ON cases
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_case_reviews_updated_at BEFORE UPDATE ON case_reviews
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
