CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

CREATE TYPE complex_type AS (
    x INTEGER,
    y INTEGER,
    z BIGINT,
    x1 NUMERIC(10, 2)
);

CREATE TABLE example_table (
    -- Numeric types
    id SERIAL PRIMARY KEY,
    small_int_col SMALLINT,
    integer_col INTEGER,
    big_int_col BIGINT,
    decimal_col DECIMAL(10, 2),
    numeric_col NUMERIC(10, 2),
    real_col REAL,
    double_col DOUBLE PRECISION,
    
    -- Character types
    char_col CHAR(10),
    varchar_col VARCHAR(255),
    text_col TEXT,
    
    -- Binary data types
    bytea_col BYTEA,
    
    -- Date/Time types
    date_col DATE,
    time_col TIME,
    timestamp_col TIMESTAMP,
    timestamptz_col TIMESTAMP WITH TIME ZONE,
    interval_col INTERVAL,
    
    -- Boolean type
    boolean_col BOOLEAN,
    
    -- Enumerated type
    mood_col mood,
    
    -- Geometric types
    point_col POINT,
    line_col LINE,
    lseg_col LSEG,
    box_col BOX,
    path_col PATH,
    polygon_col POLYGON,
    circle_col CIRCLE,
    
    -- Network address types
    inet_col INET,
    cidr_col CIDR,
    macaddr_col MACADDR,
    
    -- Bit string type
    bit_col BIT(8),
    bit_varying_col BIT VARYING(64),
    
    -- Text search types
    tsvector_col TSVECTOR,
    tsquery_col TSQUERY,
    
    -- UUID type
    uuid_col UUID,
    
    -- XML type
    xml_col XML,
    
    -- JSON types
    json_col JSON,
    jsonb_col JSONB,
    
    -- Arrays
    int_array_col INTEGER[],
    text_array_col TEXT[],
    
    -- Range types
    int_range_col INT4RANGE,
    
    -- Custom composite type
    composite_col complex_type
);
