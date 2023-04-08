--
-- PostgreSQL database dump
--

-- Dumped from database version 15.0
-- Dumped by pg_dump version 15.2

-- Started on 2023-04-08 18:43:07

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 4252 (class 1262 OID 16399)
-- Name: fyp; Type: DATABASE; Schema: -; Owner: georgia
--

CREATE DATABASE fyp WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'English_United Kingdom.1252';


ALTER DATABASE fyp OWNER TO georgia;

\connect fyp

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 2 (class 3079 OID 16407)
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- TOC entry 4253 (class 0 OID 0)
-- Dependencies: 2
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry and geography spatial types and functions';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 226 (class 1259 OID 25695)
-- Name: aston; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.aston (
    sensor_id integer NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    o3 numeric,
    no numeric,
    no2 numeric,
    pm1 numeric,
    pm10 numeric,
    "pm2.5" numeric,
    pressure numeric,
    humidity numeric,
    temperature numeric
);


ALTER TABLE public.aston OWNER TO postgres;

--
-- TOC entry 225 (class 1259 OID 25675)
-- Name: aston_sensor; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.aston_sensor (
    sensor_id integer NOT NULL,
    sensor_location public.geometry(Geometry,4326)
);


ALTER TABLE public.aston_sensor OWNER TO postgres;

--
-- TOC entry 222 (class 1259 OID 17463)
-- Name: defra; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.defra (
    reading_id integer NOT NULL,
    station_code text,
    "timestamp" timestamp with time zone,
    o3 numeric,
    no numeric,
    no2 numeric,
    nox_as_no2 numeric,
    pm10 numeric,
    "pm2.5" numeric,
    windspeed numeric,
    wind_direction numeric,
    temperature numeric,
    so2 numeric
);


ALTER TABLE public.defra OWNER TO postgres;

--
-- TOC entry 224 (class 1259 OID 17482)
-- Name: defra_reading_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.defra ALTER COLUMN reading_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.defra_reading_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 223 (class 1259 OID 17470)
-- Name: defra_station; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.defra_station (
    station_code text NOT NULL,
    station_name text,
    station_location public.geometry(Geometry,4326)
);


ALTER TABLE public.defra_station OWNER TO postgres;

--
-- TOC entry 215 (class 1259 OID 16400)
-- Name: plume_sensor; Type: TABLE; Schema: public; Owner: georgia
--

CREATE TABLE public.plume_sensor (
    id integer NOT NULL,
    utc_date timestamp(3) without time zone,
    no2 integer,
    voc integer,
    pm1 integer,
    pm10 integer,
    pm25 integer,
    latitude double precision,
    longitude double precision,
    geom public.geometry(Point,4326)
);


ALTER TABLE public.plume_sensor OWNER TO georgia;

--
-- TOC entry 216 (class 1259 OID 16403)
-- Name: plume_sensor_id_seq; Type: SEQUENCE; Schema: public; Owner: georgia
--

CREATE SEQUENCE public.plume_sensor_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.plume_sensor_id_seq OWNER TO georgia;

--
-- TOC entry 4254 (class 0 OID 0)
-- Dependencies: 216
-- Name: plume_sensor_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: georgia
--

ALTER SEQUENCE public.plume_sensor_id_seq OWNED BY public.plume_sensor.id;


--
-- TOC entry 4082 (class 2604 OID 17451)
-- Name: plume_sensor id; Type: DEFAULT; Schema: public; Owner: georgia
--

ALTER TABLE ONLY public.plume_sensor ALTER COLUMN id SET DEFAULT nextval('public.plume_sensor_id_seq'::regclass);


--
-- TOC entry 4097 (class 2606 OID 33854)
-- Name: aston aston_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.aston
    ADD CONSTRAINT aston_pkey PRIMARY KEY (sensor_id, "timestamp");


--
-- TOC entry 4095 (class 2606 OID 25681)
-- Name: aston_sensor aston_sensor_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.aston_sensor
    ADD CONSTRAINT aston_sensor_pkey PRIMARY KEY (sensor_id);


--
-- TOC entry 4089 (class 2606 OID 17469)
-- Name: defra defra_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.defra
    ADD CONSTRAINT defra_pkey PRIMARY KEY (reading_id);


--
-- TOC entry 4093 (class 2606 OID 17476)
-- Name: defra_station defra_station_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.defra_station
    ADD CONSTRAINT defra_station_pkey PRIMARY KEY (station_code);


--
-- TOC entry 4091 (class 2606 OID 33862)
-- Name: defra defra_unique_readings; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.defra
    ADD CONSTRAINT defra_unique_readings UNIQUE (station_code, "timestamp", o3, no, no2, nox_as_no2, pm10, "pm2.5", windspeed, wind_direction, temperature, so2);


--
-- TOC entry 4085 (class 2606 OID 16406)
-- Name: plume_sensor plume_sensor_pkey; Type: CONSTRAINT; Schema: public; Owner: georgia
--

ALTER TABLE ONLY public.plume_sensor
    ADD CONSTRAINT plume_sensor_pkey PRIMARY KEY (id);


--
-- TOC entry 4099 (class 2606 OID 25702)
-- Name: aston sensor_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.aston
    ADD CONSTRAINT sensor_id FOREIGN KEY (sensor_id) REFERENCES public.aston_sensor(sensor_id);


--
-- TOC entry 4098 (class 2606 OID 17477)
-- Name: defra station_code; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.defra
    ADD CONSTRAINT station_code FOREIGN KEY (station_code) REFERENCES public.defra_station(station_code) NOT VALID;


-- Completed on 2023-04-08 18:43:07

--
-- PostgreSQL database dump complete
--

