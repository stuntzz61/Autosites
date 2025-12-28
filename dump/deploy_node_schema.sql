--
-- PostgreSQL database dump
--

\restrict 8GtZ0AFmXWqIsPJ10D4D5SQoGgzh9ND2jFqH60ZcIbfqzvqzsTu8ABcgPgYTdpZ

-- Dumped from database version 17.7 (Debian 17.7-3.pgdg13+1)
-- Dumped by pg_dump version 17.7 (Debian 17.7-3.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: update_updated_at_column(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_updated_at_column() OWNER TO appuser;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: deployments; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.deployments (
    id character varying(36) NOT NULL,
    domain character varying(255),
    preview_slug character varying(20),
    server_id character varying(36) NOT NULL,
    server_name character varying(255) NOT NULL,
    server_host character varying(255),
    port integer,
    status character varying(50) DEFAULT 'pending'::character varying NOT NULL,
    archive_path text,
    build_output text,
    error_message text,
    ssl_enabled boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone
);


ALTER TABLE public.deployments OWNER TO appuser;

--
-- Name: port_allocations; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.port_allocations (
    server_id character varying(36) NOT NULL,
    port integer NOT NULL,
    domain character varying(255) NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.port_allocations OWNER TO appuser;

--
-- Name: server_health_log; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.server_health_log (
    id integer NOT NULL,
    server_id character varying(36) NOT NULL,
    cpu_usage real,
    memory_usage real,
    disk_usage real,
    active_sites integer,
    status character varying(50),
    recorded_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.server_health_log OWNER TO appuser;

--
-- Name: server_health_log_id_seq; Type: SEQUENCE; Schema: public; Owner: appuser
--

CREATE SEQUENCE public.server_health_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.server_health_log_id_seq OWNER TO appuser;

--
-- Name: server_health_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: appuser
--

ALTER SEQUENCE public.server_health_log_id_seq OWNED BY public.server_health_log.id;


--
-- Name: sites; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.sites (
    id character varying(36) NOT NULL,
    domain character varying(255) NOT NULL,
    server_id character varying(36) NOT NULL,
    container_id character varying(64),
    port integer,
    ssl_enabled boolean DEFAULT false,
    status character varying(50) DEFAULT 'active'::character varying NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_deploy timestamp with time zone
);


ALTER TABLE public.sites OWNER TO appuser;

--
-- Name: server_health_log id; Type: DEFAULT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.server_health_log ALTER COLUMN id SET DEFAULT nextval('public.server_health_log_id_seq'::regclass);


--
-- Name: deployments deployments_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.deployments
    ADD CONSTRAINT deployments_pkey PRIMARY KEY (id);


--
-- Name: port_allocations port_allocations_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.port_allocations
    ADD CONSTRAINT port_allocations_pkey PRIMARY KEY (server_id, port);


--
-- Name: server_health_log server_health_log_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.server_health_log
    ADD CONSTRAINT server_health_log_pkey PRIMARY KEY (id);


--
-- Name: sites sites_domain_key; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.sites
    ADD CONSTRAINT sites_domain_key UNIQUE (domain);


--
-- Name: sites sites_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.sites
    ADD CONSTRAINT sites_pkey PRIMARY KEY (id);


--
-- Name: idx_deployments_created; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_deployments_created ON public.deployments USING btree (created_at DESC);


--
-- Name: idx_deployments_domain; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_deployments_domain ON public.deployments USING btree (domain);


--
-- Name: idx_deployments_preview_slug; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_deployments_preview_slug ON public.deployments USING btree (preview_slug);


--
-- Name: idx_deployments_server; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_deployments_server ON public.deployments USING btree (server_id);


--
-- Name: idx_deployments_status; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_deployments_status ON public.deployments USING btree (status);


--
-- Name: idx_health_server; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_health_server ON public.server_health_log USING btree (server_id);


--
-- Name: idx_health_time; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_health_time ON public.server_health_log USING btree (recorded_at DESC);


--
-- Name: idx_ports_server; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_ports_server ON public.port_allocations USING btree (server_id);


--
-- Name: idx_sites_domain; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_sites_domain ON public.sites USING btree (domain);


--
-- Name: idx_sites_server; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_sites_server ON public.sites USING btree (server_id);


--
-- Name: idx_sites_status; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_sites_status ON public.sites USING btree (status);


--
-- Name: deployments update_deployments_updated_at; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER update_deployments_updated_at BEFORE UPDATE ON public.deployments FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: sites update_sites_updated_at; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER update_sites_updated_at BEFORE UPDATE ON public.sites FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- PostgreSQL database dump complete
--

\unrestrict 8GtZ0AFmXWqIsPJ10D4D5SQoGgzh9ND2jFqH60ZcIbfqzvqzsTu8ABcgPgYTdpZ

