PGDMP         -                {           fyp    15.0    15.2     �           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                      false            �           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                      false            �           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                      false            �           1262    16399    fyp    DATABASE        CREATE DATABASE fyp WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'English_United Kingdom.1252';
    DROP DATABASE fyp;
                georgia    false                        2615    2200    public    SCHEMA        CREATE SCHEMA public;
    DROP SCHEMA public;
                pg_database_owner    false            �           0    0    SCHEMA public    COMMENT     6   COMMENT ON SCHEMA public IS 'standard public schema';
                   pg_database_owner    false    5            �            1259    25695    aston    TABLE       CREATE TABLE public.aston (
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
    DROP TABLE public.aston;
       public         heap    postgres    false    5            �            1259    25675    aston_sensor    TABLE     y   CREATE TABLE public.aston_sensor (
    sensor_id integer NOT NULL,
    sensor_location public.geometry(Geometry,4326)
);
     DROP TABLE public.aston_sensor;
       public         heap    postgres    false    5    5    5    5    5    5    5    5    5    5    5    5    5    5    5    5    5            �            1259    17463    defra    TABLE     M  CREATE TABLE public.defra (
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
    DROP TABLE public.defra;
       public         heap    postgres    false    5            �            1259    17482    defra_reading_id_seq    SEQUENCE     �   ALTER TABLE public.defra ALTER COLUMN reading_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.defra_reading_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);
            public          postgres    false    222    5            �            1259    17470    defra_station    TABLE     �   CREATE TABLE public.defra_station (
    station_code text NOT NULL,
    station_name text,
    station_location public.geometry(Geometry,4326)
);
 !   DROP TABLE public.defra_station;
       public         heap    postgres    false    5    5    5    5    5    5    5    5    5    5    5    5    5    5    5    5    5            �            1259    16400    plume_sensor    TABLE     '  CREATE TABLE public.plume_sensor (
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
     DROP TABLE public.plume_sensor;
       public         heap    georgia    false    5    5    5    5    5    5    5    5    5    5    5    5    5    5    5    5    5            �            1259    16403    plume_sensor_id_seq    SEQUENCE     �   CREATE SEQUENCE public.plume_sensor_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
 *   DROP SEQUENCE public.plume_sensor_id_seq;
       public          georgia    false    5    215            �           0    0    plume_sensor_id_seq    SEQUENCE OWNED BY     K   ALTER SEQUENCE public.plume_sensor_id_seq OWNED BY public.plume_sensor.id;
          public          georgia    false    216            �           2604    17451    plume_sensor id    DEFAULT     r   ALTER TABLE ONLY public.plume_sensor ALTER COLUMN id SET DEFAULT nextval('public.plume_sensor_id_seq'::regclass);
 >   ALTER TABLE public.plume_sensor ALTER COLUMN id DROP DEFAULT;
       public          georgia    false    216    215                       2606    33854    aston aston_pkey 
   CONSTRAINT     b   ALTER TABLE ONLY public.aston
    ADD CONSTRAINT aston_pkey PRIMARY KEY (sensor_id, "timestamp");
 :   ALTER TABLE ONLY public.aston DROP CONSTRAINT aston_pkey;
       public            postgres    false    226    226            �           2606    25681    aston_sensor aston_sensor_pkey 
   CONSTRAINT     c   ALTER TABLE ONLY public.aston_sensor
    ADD CONSTRAINT aston_sensor_pkey PRIMARY KEY (sensor_id);
 H   ALTER TABLE ONLY public.aston_sensor DROP CONSTRAINT aston_sensor_pkey;
       public            postgres    false    225            �           2606    17469    defra defra_pkey 
   CONSTRAINT     V   ALTER TABLE ONLY public.defra
    ADD CONSTRAINT defra_pkey PRIMARY KEY (reading_id);
 :   ALTER TABLE ONLY public.defra DROP CONSTRAINT defra_pkey;
       public            postgres    false    222            �           2606    17476     defra_station defra_station_pkey 
   CONSTRAINT     h   ALTER TABLE ONLY public.defra_station
    ADD CONSTRAINT defra_station_pkey PRIMARY KEY (station_code);
 J   ALTER TABLE ONLY public.defra_station DROP CONSTRAINT defra_station_pkey;
       public            postgres    false    223            �           2606    33862    defra defra_unique_readings 
   CONSTRAINT     �   ALTER TABLE ONLY public.defra
    ADD CONSTRAINT defra_unique_readings UNIQUE (station_code, "timestamp", o3, no, no2, nox_as_no2, pm10, "pm2.5", windspeed, wind_direction, temperature, so2);
 E   ALTER TABLE ONLY public.defra DROP CONSTRAINT defra_unique_readings;
       public            postgres    false    222    222    222    222    222    222    222    222    222    222    222    222            �           2606    16406    plume_sensor plume_sensor_pkey 
   CONSTRAINT     \   ALTER TABLE ONLY public.plume_sensor
    ADD CONSTRAINT plume_sensor_pkey PRIMARY KEY (id);
 H   ALTER TABLE ONLY public.plume_sensor DROP CONSTRAINT plume_sensor_pkey;
       public            georgia    false    215                       2606    25702    aston sensor_id    FK CONSTRAINT     ~   ALTER TABLE ONLY public.aston
    ADD CONSTRAINT sensor_id FOREIGN KEY (sensor_id) REFERENCES public.aston_sensor(sensor_id);
 9   ALTER TABLE ONLY public.aston DROP CONSTRAINT sensor_id;
       public          postgres    false    4095    225    226                       2606    17477    defra station_code    FK CONSTRAINT     �   ALTER TABLE ONLY public.defra
    ADD CONSTRAINT station_code FOREIGN KEY (station_code) REFERENCES public.defra_station(station_code) NOT VALID;
 <   ALTER TABLE ONLY public.defra DROP CONSTRAINT station_code;
       public          postgres    false    222    223    4093           