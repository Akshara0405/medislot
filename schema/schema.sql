-- =====================================================================================
-- MediSlot MySQL schema: the ONE place where tables are defined.
--
-- It is applied once per deployment by the authentication Lambda's "migrate" action
-- (see lambdas/authentication/migrate.py and the "Apply database schema" step in
-- .github/workflows/deploy.yaml). Normal API requests never run DDL any more.
--
-- Rules
--   * Every id is BIGINT UNSIGNED AUTO_INCREMENT (no UUIDs).
--   * Every statement is safe to run again (IF NOT EXISTS / WHERE NOT EXISTS).
--   * Statements end with ";" at the end of a line; the migrator splits on that.
-- =====================================================================================

-- ---------------------------------------------------------------- identity
CREATE TABLE IF NOT EXISTS users (
  user_id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  email          VARCHAR(254)    NOT NULL,
  password_hash  VARCHAR(255)    NOT NULL,
  role           ENUM('patient','doctor') NOT NULL,
  is_active      BOOLEAN         NOT NULL DEFAULT TRUE,
  created_at     DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at     DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (user_id),
  UNIQUE KEY uq_users_email (email),
  KEY ix_users_role_active (role, is_active)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS hospitals (
  hospital_id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name         VARCHAR(150)    NOT NULL,
  address      VARCHAR(500)    NULL,
  city         VARCHAR(100)    NULL,
  is_active    BOOLEAN         NOT NULL DEFAULT TRUE,
  created_at   DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (hospital_id),
  UNIQUE KEY uq_hospitals_name_city (name, city),
  KEY ix_hospitals_city (city, is_active)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS patients (
  user_id                  BIGINT UNSIGNED NOT NULL,
  first_name               VARCHAR(75)     NOT NULL,
  last_name                VARCHAR(75)     NOT NULL,
  phone                    VARCHAR(25)     NOT NULL,
  date_of_birth            DATE            NULL,
  gender                   VARCHAR(30)     NULL,
  address                  VARCHAR(500)    NULL,
  city                     VARCHAR(100)    NULL,
  blood_group              VARCHAR(3)      NULL,
  emergency_contact_name   VARCHAR(150)    NULL,
  emergency_contact_phone  VARCHAR(25)     NULL,
  profile_version          INT UNSIGNED    NOT NULL DEFAULT 1,
  created_at               DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at               DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (user_id),
  CONSTRAINT fk_patients_user FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Search indexes: InnoDB appends the primary key (user_id) to every secondary index,
-- so each one already ends in the keyset-pagination tie-breaker.
CREATE TABLE IF NOT EXISTS doctors (
  user_id           BIGINT UNSIGNED   NOT NULL,
  first_name        VARCHAR(75)       NOT NULL,
  last_name         VARCHAR(75)       NOT NULL,
  phone             VARCHAR(25)       NOT NULL,
  specialization    VARCHAR(120)      NOT NULL,
  license_number    VARCHAR(32)       NOT NULL,
  hospital_id       BIGINT UNSIGNED   NOT NULL,
  qualification     VARCHAR(150)      NOT NULL,
  experience_years  SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  timezone          VARCHAR(64)       NOT NULL DEFAULT 'Asia/Kolkata',
  languages         VARCHAR(200)      NULL,
  photo_key         VARCHAR(300)      NULL,
  created_at        DATETIME(6)       NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (user_id),
  UNIQUE KEY uq_doctors_license (license_number),
  KEY ix_doctors_name (last_name, first_name),
  KEY ix_doctors_first_name (first_name),
  KEY ix_doctors_spec_name (specialization, last_name, first_name),
  KEY ix_doctors_hospital_name (hospital_id, last_name, first_name),
  CONSTRAINT fk_doctors_user     FOREIGN KEY (user_id)     REFERENCES users (user_id) ON DELETE CASCADE,
  CONSTRAINT fk_doctors_hospital FOREIGN KEY (hospital_id) REFERENCES hospitals (hospital_id)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------- scheduling
-- Master list of consultation kinds (replaces the old ENUM('QUICK','STANDARD','DETAILED','HEALTH_CAMP')).
-- Add a new kind with one INSERT; no code or ALTER TABLE needed.
CREATE TABLE IF NOT EXISTS consultations (
  consultation_id  BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  code             VARCHAR(30)      NOT NULL,
  label            VARCHAR(60)      NOT NULL,
  duration_min     SMALLINT UNSIGNED NOT NULL,
  min_capacity     TINYINT UNSIGNED NOT NULL DEFAULT 1,
  max_capacity     TINYINT UNSIGNED NOT NULL DEFAULT 1,
  sort_order       TINYINT UNSIGNED NOT NULL DEFAULT 0,
  is_active        BOOLEAN          NOT NULL DEFAULT TRUE,
  PRIMARY KEY (consultation_id),
  UNIQUE KEY uq_consultations_code (code)
) ENGINE=InnoDB;

-- What one doctor offers: which consultation, at what fee and seats per slot.
CREATE TABLE IF NOT EXISTS consultation_types (
  consultation_type_id  BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  doctor_id             BIGINT UNSIGNED  NOT NULL,
  consultation_id       BIGINT UNSIGNED  NOT NULL,
  fee_amount            DECIMAL(10,2)    NOT NULL,
  capacity              TINYINT UNSIGNED NOT NULL DEFAULT 1,
  is_active             BOOLEAN          NOT NULL DEFAULT TRUE,
  PRIMARY KEY (consultation_type_id),
  UNIQUE KEY uq_doctor_consultation (doctor_id, consultation_id),
  CONSTRAINT fk_ct_doctor       FOREIGN KEY (doctor_id)       REFERENCES doctors (user_id),
  CONSTRAINT fk_ct_consultation FOREIGN KEY (consultation_id) REFERENCES consultations (consultation_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS availability_patterns (
  pattern_id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  doctor_id             BIGINT UNSIGNED NOT NULL,
  consultation_type_id  BIGINT UNSIGNED NOT NULL,
  day_of_week           TINYINT         NOT NULL,   -- Monday = 0
  start_time            TIME            NOT NULL,
  end_time              TIME            NOT NULL,
  PRIMARY KEY (pattern_id),
  KEY ix_doctor_weekday (doctor_id, day_of_week),
  CONSTRAINT fk_ap_doctor FOREIGN KEY (doctor_id)            REFERENCES doctors (user_id),
  CONSTRAINT fk_ap_type   FOREIGN KEY (consultation_type_id) REFERENCES consultation_types (consultation_type_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS blackout_dates (
  blackout_id    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  doctor_id      BIGINT UNSIGNED NOT NULL,
  blackout_date  DATE            NOT NULL,
  reason         VARCHAR(200)    NULL,
  PRIMARY KEY (blackout_id),
  UNIQUE KEY uq_doctor_blackout (doctor_id, blackout_date),
  CONSTRAINT fk_bd_doctor FOREIGN KEY (doctor_id) REFERENCES doctors (user_id)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------- seed data
-- One single-row INSERT per row, and only when the row is missing. Unlike INSERT IGNORE,
-- this never uses up an AUTO_INCREMENT number when the row already exists.
INSERT INTO consultations (code, label, duration_min, min_capacity, max_capacity, sort_order)
  SELECT 'QUICK', 'Quick consult', 15, 1, 1, 1 FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM consultations WHERE code = 'QUICK');
INSERT INTO consultations (code, label, duration_min, min_capacity, max_capacity, sort_order)
  SELECT 'STANDARD', 'Standard consult', 30, 1, 1, 2 FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM consultations WHERE code = 'STANDARD');
INSERT INTO consultations (code, label, duration_min, min_capacity, max_capacity, sort_order)
  SELECT 'DETAILED', 'Detailed consult', 60, 1, 1, 3 FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM consultations WHERE code = 'DETAILED');
INSERT INTO consultations (code, label, duration_min, min_capacity, max_capacity, sort_order)
  SELECT 'HEALTH_CAMP', 'Health camp', 60, 2, 10, 4 FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM consultations WHERE code = 'HEALTH_CAMP');

INSERT INTO hospitals (name, city, address)
  SELECT 'MediSlot General Hospital', 'Hyderabad', 'Main Road, Hyderabad' FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM hospitals WHERE name = 'MediSlot General Hospital' AND city = 'Hyderabad');
INSERT INTO hospitals (name, city, address)
  SELECT 'Apollo Hospitals', 'Hyderabad', 'Jubilee Hills, Hyderabad' FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM hospitals WHERE name = 'Apollo Hospitals' AND city = 'Hyderabad');
INSERT INTO hospitals (name, city, address)
  SELECT 'Yashoda Hospitals', 'Hyderabad', 'Secunderabad, Hyderabad' FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM hospitals WHERE name = 'Yashoda Hospitals' AND city = 'Hyderabad');
INSERT INTO hospitals (name, city, address)
  SELECT 'KIMS Hospitals', 'Hyderabad', 'Secunderabad, Hyderabad' FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM hospitals WHERE name = 'KIMS Hospitals' AND city = 'Hyderabad');
INSERT INTO hospitals (name, city, address)
  SELECT 'CARE Hospitals', 'Hyderabad', 'Banjara Hills, Hyderabad' FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM hospitals WHERE name = 'CARE Hospitals' AND city = 'Hyderabad');
INSERT INTO hospitals (name, city, address)
  SELECT 'Continental Hospitals', 'Hyderabad', 'Gachibowli, Hyderabad' FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM hospitals WHERE name = 'Continental Hospitals' AND city = 'Hyderabad');
INSERT INTO hospitals (name, city, address)
  SELECT 'AIG Hospitals', 'Hyderabad', 'Gachibowli, Hyderabad' FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM hospitals WHERE name = 'AIG Hospitals' AND city = 'Hyderabad');
INSERT INTO hospitals (name, city, address)
  SELECT 'Nizam''s Institute of Medical Sciences', 'Hyderabad', 'Punjagutta, Hyderabad' FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM hospitals WHERE name = 'Nizam''s Institute of Medical Sciences' AND city = 'Hyderabad');
INSERT INTO hospitals (name, city, address)
  SELECT 'Osmania General Hospital', 'Hyderabad', 'Afzalgunj, Hyderabad' FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM hospitals WHERE name = 'Osmania General Hospital' AND city = 'Hyderabad');
