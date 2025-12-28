--
-- PostgreSQL database dump
--

\restrict TBbh1GTUaxts1WbASsjAEqYWuA7hiqPkynbZoJ2RKnPyQndlHa0mBi869v1Pj7z

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
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- Name: count_new_feedback(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.count_new_feedback() RETURNS bigint
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM manager_feedback WHERE status = 'new');
END;
$$;


ALTER FUNCTION public.count_new_feedback() OWNER TO appuser;

--
-- Name: count_pending_approvals(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.count_pending_approvals() RETURNS bigint
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM users WHERE approval_status = 'pending' AND role = 'manager');
END;
$$;


ALTER FUNCTION public.count_pending_approvals() OWNER TO appuser;

--
-- Name: get_manager_stats(uuid); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.get_manager_stats(manager_uuid uuid) RETURNS TABLE(total_requests bigint, pending_requests bigint, completed_requests bigint, failed_requests bigint, total_photos bigint, avg_completion_time interval)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*)::BIGINT as total_requests,
        COUNT(*) FILTER (WHERE r.status IN ('draft', 'collecting_info', 'collecting_photos', 'ready_to_generate', 'queued'))::BIGINT as pending_requests,
        COUNT(*) FILTER (WHERE r.status = 'generated_ok')::BIGINT as completed_requests,
        COUNT(*) FILTER (WHERE r.status = 'generated_error')::BIGINT as failed_requests,
        COALESCE(SUM(jsonb_array_length(COALESCE(r.payload_json->'site'->'assets'->'images', '[]'::jsonb))), 0)::BIGINT as total_photos,
        AVG(r.generation_completed_at - r.generation_started_at) FILTER (WHERE r.generation_completed_at IS NOT NULL) as avg_completion_time
    FROM requests r
    JOIN projects p ON p.id = r.project_id
    WHERE p.manager_id = manager_uuid;
END;
$$;


ALTER FUNCTION public.get_manager_stats(manager_uuid uuid) OWNER TO appuser;

--
-- Name: get_next_revision_iteration(uuid); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.get_next_revision_iteration(p_site_id uuid) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    next_iteration INTEGER;
BEGIN
    SELECT COALESCE(MAX(iteration), 0) + 1 INTO next_iteration
    FROM revisions
    WHERE site_id = p_site_id;

    RETURN next_iteration;
END;
$$;


ALTER FUNCTION public.get_next_revision_iteration(p_site_id uuid) OWNER TO appuser;

--
-- Name: FUNCTION get_next_revision_iteration(p_site_id uuid); Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON FUNCTION public.get_next_revision_iteration(p_site_id uuid) IS 'Возвращает следующий номер итерации для сайта';


--
-- Name: get_overall_stats(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.get_overall_stats() RETURNS TABLE(total_users bigint, total_managers bigint, total_requests bigint, requests_today bigint, requests_this_week bigint, requests_this_month bigint, pending_generation bigint, completed_today bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        (SELECT COUNT(*) FROM users)::BIGINT,
        (SELECT COUNT(*) FROM users WHERE role = 'manager')::BIGINT,
        (SELECT COUNT(*) FROM requests)::BIGINT,
        (SELECT COUNT(*) FROM requests WHERE created_at >= CURRENT_DATE)::BIGINT,
        (SELECT COUNT(*) FROM requests WHERE created_at >= CURRENT_DATE - INTERVAL '7 days')::BIGINT,
        (SELECT COUNT(*) FROM requests WHERE created_at >= CURRENT_DATE - INTERVAL '30 days')::BIGINT,
        (SELECT COUNT(*) FROM requests WHERE status IN ('queued', 'generating'))::BIGINT,
        (SELECT COUNT(*) FROM requests WHERE status = 'generated_ok' AND generation_completed_at >= CURRENT_DATE)::BIGINT;
END;
$$;


ALTER FUNCTION public.get_overall_stats() OWNER TO appuser;

--
-- Name: set_updated_at(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;


ALTER FUNCTION public.set_updated_at() OWNER TO appuser;

--
-- Name: should_send_payment_warning(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.should_send_payment_warning() RETURNS TABLE(site_id uuid, company_name character varying, manager_tg_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    cs.id,
    cs.company_name,
    u.tg_id
  FROM client_sites cs
  JOIN users u ON u.id = cs.manager_id
  WHERE cs.deploy_status = 'active'
    AND cs.hosting_expires_at IS NOT NULL
    AND cs.hosting_expires_at <= NOW() + INTERVAL '14 days'
    AND cs.hosting_expires_at > NOW()
    AND (cs.payment_warning_sent_at IS NULL OR cs.payment_warning_sent_at < cs.hosting_expires_at - INTERVAL '13 days');
END;
$$;


ALTER FUNCTION public.should_send_payment_warning() OWNER TO appuser;

--
-- Name: sites_to_auto_disable(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.sites_to_auto_disable() RETURNS TABLE(site_id uuid, company_name character varying, manager_tg_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    cs.id,
    cs.company_name,
    u.tg_id
  FROM client_sites cs
  JOIN users u ON u.id = cs.manager_id
  WHERE cs.deploy_status = 'active'
    AND cs.hosting_expires_at IS NOT NULL
    AND cs.hosting_expires_at < NOW() - INTERVAL '14 days'
    AND cs.auto_disabled_at IS NULL;
END;
$$;


ALTER FUNCTION public.sites_to_auto_disable() OWNER TO appuser;

--
-- Name: sites_to_delete(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.sites_to_delete() RETURNS TABLE(site_id uuid, company_name character varying, manager_tg_id bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
  RETURN QUERY
  SELECT
    cs.id,
    cs.company_name,
    u.tg_id
  FROM client_sites cs
  JOIN users u ON u.id = cs.manager_id
  WHERE cs.deploy_status IN ('active', 'stopped')
    AND cs.hosting_expires_at IS NOT NULL
    AND cs.hosting_expires_at < NOW() - INTERVAL '60 days'
    AND cs.scheduled_for_deletion_at IS NOT NULL
    AND cs.scheduled_for_deletion_at <= NOW();
END;
$$;


ALTER FUNCTION public.sites_to_delete() OWNER TO appuser;

--
-- Name: trigger_set_timestamp(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.trigger_set_timestamp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;


ALTER FUNCTION public.trigger_set_timestamp() OWNER TO appuser;

--
-- Name: update_invite_codes_updated_at(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.update_invite_codes_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_invite_codes_updated_at() OWNER TO appuser;

--
-- Name: update_revision_updated_at(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.update_revision_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_revision_updated_at() OWNER TO appuser;

--
-- Name: update_site_revision_count(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.update_site_revision_count() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE client_sites
        SET revision_count = revision_count + 1,
            current_revision_id = NEW.id,
            revision_status = NEW.status
        WHERE id = NEW.site_id;
    ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
        UPDATE client_sites
        SET revision_status = NEW.status
        WHERE id = NEW.site_id AND current_revision_id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_site_revision_count() OWNER TO appuser;

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

--
-- Name: update_workspaces_updated_at(); Type: FUNCTION; Schema: public; Owner: appuser
--

CREATE FUNCTION public.update_workspaces_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_workspaces_updated_at() OWNER TO appuser;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: client_sites; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.client_sites (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    request_id uuid,
    manager_id uuid NOT NULL,
    company_name character varying(255) NOT NULL,
    client_name character varying(255),
    client_contact character varying(255),
    deploy_id character varying(36),
    preview_slug character varying(50),
    preview_url text,
    domain character varying(255),
    domain_status character varying(30) DEFAULT 'none'::character varying,
    ssl_enabled boolean DEFAULT false,
    generation_status character varying(30) DEFAULT 'pending'::character varying,
    deploy_status character varying(30) DEFAULT 'none'::character varying,
    hosting_plan character varying(30) DEFAULT 'trial'::character varying,
    hosting_expires_at timestamp without time zone,
    hosting_auto_renew boolean DEFAULT false,
    archive_s3_key text,
    archive_size_bytes bigint,
    server_id character varying(36),
    server_name character varying(255),
    server_host character varying(255),
    container_port integer,
    last_error text,
    last_error_at timestamp without time zone,
    notes text,
    metadata jsonb DEFAULT '{}'::jsonb,
    generated_at timestamp without time zone,
    deployed_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    payment_warning_sent_at timestamp without time zone,
    auto_disabled_at timestamp without time zone,
    scheduled_for_deletion_at timestamp without time zone,
    revision_count integer DEFAULT 0,
    current_revision_id uuid,
    revision_status character varying(50),
    cms_site_id uuid
);


ALTER TABLE public.client_sites OWNER TO appuser;

--
-- Name: TABLE client_sites; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.client_sites IS 'Сайты клиентов: связь заявок с деплоями и хостингом';


--
-- Name: COLUMN client_sites.request_id; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.client_sites.request_id IS 'Optional: linked request. NULL for imported sites without request.';


--
-- Name: COLUMN client_sites.deploy_id; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.client_sites.deploy_id IS 'ID деплоя в main-deploy-node';


--
-- Name: COLUMN client_sites.preview_slug; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.client_sites.preview_slug IS 'Slug для preview URL (abc12345)';


--
-- Name: COLUMN client_sites.payment_warning_sent_at; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.client_sites.payment_warning_sent_at IS 'Когда отправлено предупреждение об истечении (за 2 недели)';


--
-- Name: COLUMN client_sites.auto_disabled_at; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.client_sites.auto_disabled_at IS 'Когда сайт был автоматически отключен (через 2 недели после истечения)';


--
-- Name: COLUMN client_sites.scheduled_for_deletion_at; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.client_sites.scheduled_for_deletion_at IS 'Когда сайт будет удален (через 2 месяца после истечения)';


--
-- Name: COLUMN client_sites.revision_count; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.client_sites.revision_count IS 'Количество итераций правок по сайту';


--
-- Name: COLUMN client_sites.current_revision_id; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.client_sites.current_revision_id IS 'ID текущей активной правки';


--
-- Name: COLUMN client_sites.revision_status; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.client_sites.revision_status IS 'Статус текущей правки: pending, in_progress, completed';


--
-- Name: revision_changes; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.revision_changes (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    revision_id uuid NOT NULL,
    change_type character varying(50) DEFAULT 'text_change'::character varying NOT NULL,
    location_area character varying(100),
    location_selector character varying(255),
    location_description text,
    client_description text NOT NULL,
    old_value text,
    new_value_suggestion text,
    screenshot_s3_key character varying(500),
    screenshot_comment text,
    priority character varying(20) DEFAULT 'normal'::character varying,
    status character varying(50) DEFAULT 'pending'::character varying,
    ai_interpretation text,
    ai_confidence double precision,
    metadata jsonb DEFAULT '{}'::jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.revision_changes OWNER TO appuser;

--
-- Name: TABLE revision_changes; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.revision_changes IS 'Отдельные правки внутри итерации';


--
-- Name: COLUMN revision_changes.change_type; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.revision_changes.change_type IS 'Тип правки: text_change, visual_change, layout_change, etc.';


--
-- Name: COLUMN revision_changes.location_area; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.revision_changes.location_area IS 'Область сайта: hero, header, footer, about, etc.';


--
-- Name: revisions; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.revisions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    site_id uuid NOT NULL,
    request_id uuid,
    iteration integer DEFAULT 1 NOT NULL,
    status character varying(50) DEFAULT 'pending'::character varying NOT NULL,
    s3_folder character varying(500),
    archive_s3_key character varying(500),
    result_archive_s3_key character varying(500),
    n8n_job_id character varying(255),
    n8n_webhook_url character varying(500),
    n8n_sent_at timestamp with time zone,
    n8n_response_at timestamp with time zone,
    error_message text,
    error_details jsonb,
    source character varying(50) DEFAULT 'telegram_bot'::character varying,
    client_id character varying(255),
    manager_id uuid,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    completed_at timestamp with time zone
);


ALTER TABLE public.revisions OWNER TO appuser;

--
-- Name: TABLE revisions; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.revisions IS 'Итерации правок сайтов';


--
-- Name: COLUMN revisions.iteration; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.revisions.iteration IS 'Номер итерации правок (1, 2, 3, ...)';


--
-- Name: COLUMN revisions.s3_folder; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.revisions.s3_folder IS 'Путь к папке в S3 с файлами правок';


--
-- Name: COLUMN revisions.n8n_job_id; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.revisions.n8n_job_id IS 'ID задачи в n8n для корреляции запроса/ответа';


--
-- Name: users; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.users (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tg_id bigint NOT NULL,
    role text NOT NULL,
    first_name text,
    last_name text,
    contact text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    approval_status character varying(20) DEFAULT 'pending'::character varying,
    approved_by uuid,
    approved_at timestamp without time zone,
    rejection_reason text,
    username character varying(100),
    is_blocked boolean DEFAULT false,
    admin_group_id uuid,
    registered_via_code uuid,
    full_name character varying(255),
    phone character varying(50),
    phone_verified boolean DEFAULT false,
    registration_completed_at timestamp with time zone,
    workspace_id uuid,
    email character varying(255),
    CONSTRAINT users_role_check CHECK ((role = ANY (ARRAY['guest'::text, 'manager'::text, 'supervisor'::text, 'director'::text, 'owner'::text])))
);


ALTER TABLE public.users OWNER TO appuser;

--
-- Name: COLUMN users.role; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.users.role IS 'User role hierarchy: owner (владелец), director (директор), supervisor (супервайзер/team lead), manager (менеджер), guest';


--
-- Name: COLUMN users.approval_status; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.users.approval_status IS 'Статус одобрения: pending, approved, rejected';


--
-- Name: COLUMN users.is_blocked; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.users.is_blocked IS 'Флаг блокировки пользователя';


--
-- Name: COLUMN users.registered_via_code; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.users.registered_via_code IS 'Reference to the invite code used during registration';


--
-- Name: COLUMN users.full_name; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.users.full_name IS 'Manager full name (FIO) for registration';


--
-- Name: COLUMN users.phone; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.users.phone IS 'Manager phone number';


--
-- Name: COLUMN users.registration_completed_at; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.users.registration_completed_at IS 'When registration form was completed';


--
-- Name: COLUMN users.workspace_id; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.users.workspace_id IS 'Workspace/tenant this user belongs to';


--
-- Name: COLUMN users.email; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.users.email IS 'Manager email address for registration';


--
-- Name: active_revisions; Type: VIEW; Schema: public; Owner: appuser
--

CREATE VIEW public.active_revisions AS
 SELECT r.id,
    r.site_id,
    r.request_id,
    r.iteration,
    r.status,
    r.s3_folder,
    r.archive_s3_key,
    r.result_archive_s3_key,
    r.n8n_job_id,
    r.n8n_webhook_url,
    r.n8n_sent_at,
    r.n8n_response_at,
    r.error_message,
    r.error_details,
    r.source,
    r.client_id,
    r.manager_id,
    r.created_at,
    r.updated_at,
    r.completed_at,
    cs.company_name,
    cs.preview_url,
    cs.domain,
    cs.deploy_status,
    u.first_name AS manager_first_name,
    u.last_name AS manager_last_name,
    u.tg_id AS manager_tg_id,
    ( SELECT count(*) AS count
           FROM public.revision_changes
          WHERE (revision_changes.revision_id = r.id)) AS changes_count
   FROM ((public.revisions r
     JOIN public.client_sites cs ON ((cs.id = r.site_id)))
     LEFT JOIN public.users u ON ((u.id = r.manager_id)))
  WHERE ((r.status)::text = ANY ((ARRAY['pending'::character varying, 'in_progress'::character varying, 'processing'::character varying])::text[]))
  ORDER BY r.created_at DESC;


ALTER VIEW public.active_revisions OWNER TO appuser;

--
-- Name: VIEW active_revisions; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON VIEW public.active_revisions IS 'Активные правки в работе';


--
-- Name: activity_log; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.activity_log (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid,
    action character varying(50) NOT NULL,
    entity_type character varying(30),
    entity_id uuid,
    details jsonb DEFAULT '{}'::jsonb,
    ip_address character varying(45),
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.activity_log OWNER TO appuser;

--
-- Name: TABLE activity_log; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.activity_log IS 'Лог всех действий пользователей для аудита';


--
-- Name: additional_services; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.additional_services (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    code character varying(50) NOT NULL,
    name character varying(255) NOT NULL,
    description text,
    price_info character varying(255),
    icon character varying(50),
    is_active boolean DEFAULT true,
    sort_order integer DEFAULT 0,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    photo_required boolean DEFAULT false
);


ALTER TABLE public.additional_services OWNER TO appuser;

--
-- Name: TABLE additional_services; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.additional_services IS 'Справочник дополнительных услуг';


--
-- Name: admin_groups; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.admin_groups (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name character varying(255) NOT NULL,
    description text,
    created_by uuid,
    is_active boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.admin_groups OWNER TO appuser;

--
-- Name: TABLE admin_groups; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.admin_groups IS 'Groups linking admins with managers they can manage';


--
-- Name: admin_notifications; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.admin_notifications (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    notification_type character varying(50) NOT NULL,
    title text NOT NULL,
    message text,
    entity_type character varying(30),
    entity_id uuid,
    is_read boolean DEFAULT false,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.admin_notifications OWNER TO appuser;

--
-- Name: TABLE admin_notifications; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.admin_notifications IS 'Уведомления для администраторов';


--
-- Name: admin_settings; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.admin_settings (
    id integer NOT NULL,
    key character varying(100) NOT NULL,
    value text NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.admin_settings OWNER TO appuser;

--
-- Name: admin_settings_id_seq; Type: SEQUENCE; Schema: public; Owner: appuser
--

CREATE SEQUENCE public.admin_settings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.admin_settings_id_seq OWNER TO appuser;

--
-- Name: admin_settings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: appuser
--

ALTER SEQUENCE public.admin_settings_id_seq OWNED BY public.admin_settings.id;


--
-- Name: agent_runs; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.agent_runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    project_id uuid,
    version_id uuid,
    type text NOT NULL,
    input_ref jsonb,
    output_ref jsonb,
    status text DEFAULT 'queued'::text NOT NULL,
    cost_tokens integer,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_runs_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'succeeded'::text, 'failed'::text]))),
    CONSTRAINT agent_runs_type_check CHECK ((type = ANY (ARRAY['prompt'::text, 'generate'::text, 'qa'::text, 'patch'::text])))
);


ALTER TABLE public.agent_runs OWNER TO appuser;

--
-- Name: anti_nuke_settings; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.anti_nuke_settings (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    setting_key character varying(100) NOT NULL,
    setting_value text NOT NULL,
    description text,
    updated_by uuid,
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.anti_nuke_settings OWNER TO appuser;

--
-- Name: TABLE anti_nuke_settings; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.anti_nuke_settings IS 'Configuration for anti-nuke protection';


--
-- Name: change_requests; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.change_requests (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    project_id uuid NOT NULL,
    base_version_id uuid,
    client_text text NOT NULL,
    parsed_actions_json jsonb,
    status text DEFAULT 'new'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT change_requests_status_check CHECK ((status = ANY (ARRAY['new'::text, 'in_progress'::text, 'done'::text, 'rejected'::text])))
);


ALTER TABLE public.change_requests OWNER TO appuser;

--
-- Name: current_request; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.current_request (
    tgid bigint NOT NULL,
    request_id uuid NOT NULL
);


ALTER TABLE public.current_request OWNER TO appuser;

--
-- Name: daily_stats; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.daily_stats (
    stat_date date NOT NULL,
    total_requests integer DEFAULT 0,
    completed_requests integer DEFAULT 0,
    failed_requests integer DEFAULT 0,
    new_users integer DEFAULT 0,
    active_managers integer DEFAULT 0,
    photos_uploaded integer DEFAULT 0,
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.daily_stats OWNER TO appuser;

--
-- Name: TABLE daily_stats; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.daily_stats IS 'Ежедневная агрегированная статистика';


--
-- Name: deletion_audit_log; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.deletion_audit_log (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    action_type character varying(50) NOT NULL,
    target_type character varying(50) NOT NULL,
    target_id uuid,
    target_ids uuid[],
    target_count integer DEFAULT 1,
    performed_by uuid NOT NULL,
    reason text,
    ip_address character varying(45),
    user_agent text,
    metadata jsonb DEFAULT '{}'::jsonb,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.deletion_audit_log OWNER TO appuser;

--
-- Name: TABLE deletion_audit_log; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.deletion_audit_log IS 'Audit log for all deletion operations (anti-nuke)';


--
-- Name: deploy_history; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.deploy_history (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    client_site_id uuid NOT NULL,
    deploy_id character varying(36) NOT NULL,
    action character varying(30) NOT NULL,
    status character varying(30) NOT NULL,
    archive_s3_key text,
    build_output text,
    error_message text,
    initiated_by uuid,
    started_at timestamp without time zone DEFAULT now(),
    completed_at timestamp without time zone
);


ALTER TABLE public.deploy_history OWNER TO appuser;

--
-- Name: TABLE deploy_history; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.deploy_history IS 'История деплоев для аудита и rollback';


--
-- Name: expiring_sites; Type: VIEW; Schema: public; Owner: appuser
--

CREATE VIEW public.expiring_sites AS
 SELECT cs.id,
    cs.company_name,
    cs.domain,
    cs.preview_url,
    cs.hosting_plan,
    cs.hosting_expires_at,
    cs.hosting_auto_renew,
    cs.manager_id,
    u.first_name AS manager_first_name,
    u.last_name AS manager_last_name,
    u.tg_id AS manager_tg_id,
    EXTRACT(day FROM ((cs.hosting_expires_at)::timestamp with time zone - now())) AS days_remaining
   FROM (public.client_sites cs
     JOIN public.users u ON ((u.id = cs.manager_id)))
  WHERE (((cs.deploy_status)::text = 'active'::text) AND (cs.hosting_expires_at IS NOT NULL) AND (cs.hosting_expires_at <= (now() + '7 days'::interval)))
  ORDER BY cs.hosting_expires_at;


ALTER VIEW public.expiring_sites OWNER TO appuser;

--
-- Name: VIEW expiring_sites; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON VIEW public.expiring_sites IS 'Sites with hosting expiring within 14 days';


--
-- Name: manager_feedback; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.manager_feedback (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    manager_id uuid NOT NULL,
    subject character varying(255) NOT NULL,
    message text NOT NULL,
    category character varying(50) DEFAULT 'general'::character varying,
    priority character varying(20) DEFAULT 'normal'::character varying,
    status character varying(30) DEFAULT 'new'::character varying,
    request_id uuid,
    admin_response text,
    responded_by uuid,
    responded_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.manager_feedback OWNER TO appuser;

--
-- Name: TABLE manager_feedback; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.manager_feedback IS 'Обратная связь от менеджеров к администраторам';


--
-- Name: feedback_with_manager; Type: VIEW; Schema: public; Owner: appuser
--

CREATE VIEW public.feedback_with_manager AS
 SELECT f.id,
    f.subject,
    f.message,
    f.category,
    f.priority,
    f.status,
    f.request_id,
    f.admin_response,
    f.responded_at,
    f.created_at,
    f.updated_at,
    u.id AS manager_id,
    u.tg_id AS manager_tg_id,
    u.first_name AS manager_first_name,
    u.last_name AS manager_last_name,
    u.username AS manager_username,
    ru.first_name AS responder_first_name,
    ru.last_name AS responder_last_name
   FROM ((public.manager_feedback f
     JOIN public.users u ON ((u.id = f.manager_id)))
     LEFT JOIN public.users ru ON ((ru.id = f.responded_by)))
  ORDER BY
        CASE f.status
            WHEN 'new'::text THEN 0
            ELSE 1
        END,
        CASE f.priority
            WHEN 'urgent'::text THEN 0
            WHEN 'high'::text THEN 1
            WHEN 'normal'::text THEN 2
            ELSE 3
        END, f.created_at DESC;


ALTER VIEW public.feedback_with_manager OWNER TO appuser;

--
-- Name: files; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.files (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    project_id uuid,
    version_id uuid,
    kind text NOT NULL,
    s3_key text NOT NULL,
    size_bytes bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT files_kind_check CHECK ((kind = ANY (ARRAY['zip'::text, 'report'::text, 'image'::text, 'asset'::text])))
);


ALTER TABLE public.files OWNER TO appuser;

--
-- Name: hosting_plans; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.hosting_plans (
    id character varying(30) NOT NULL,
    name character varying(100) NOT NULL,
    description text,
    price_monthly numeric(10,2),
    price_yearly numeric(10,2),
    max_sites integer DEFAULT 1,
    storage_gb integer DEFAULT 1,
    bandwidth_gb integer DEFAULT 10,
    custom_domain boolean DEFAULT false,
    ssl_included boolean DEFAULT false,
    priority_support boolean DEFAULT false,
    trial_days integer DEFAULT 7,
    is_active boolean DEFAULT true,
    sort_order integer DEFAULT 0
);


ALTER TABLE public.hosting_plans OWNER TO appuser;

--
-- Name: TABLE hosting_plans; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.hosting_plans IS 'Тарифные планы хостинга';


--
-- Name: hosting_transactions; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.hosting_transactions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    client_site_id uuid NOT NULL,
    type character varying(30) NOT NULL,
    amount numeric(10,2) NOT NULL,
    currency character varying(3) DEFAULT 'RUB'::character varying,
    plan_id character varying(30),
    period_months integer DEFAULT 1,
    status character varying(30) DEFAULT 'pending'::character varying,
    payment_method character varying(50),
    external_id character varying(255),
    notes text,
    metadata jsonb DEFAULT '{}'::jsonb,
    created_at timestamp without time zone DEFAULT now(),
    completed_at timestamp without time zone,
    qr_code_url text,
    payment_url text,
    qr_image_data text,
    expires_at timestamp without time zone,
    verified_at timestamp without time zone,
    payment_system character varying(50) DEFAULT 'manual'::character varying
);


ALTER TABLE public.hosting_transactions OWNER TO appuser;

--
-- Name: TABLE hosting_transactions; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.hosting_transactions IS 'Платежи за хостинг';


--
-- Name: COLUMN hosting_transactions.qr_code_url; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.hosting_transactions.qr_code_url IS 'URL QR кода для оплаты';


--
-- Name: COLUMN hosting_transactions.payment_url; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.hosting_transactions.payment_url IS 'URL для прямого перехода к оплате';


--
-- Name: COLUMN hosting_transactions.expires_at; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.hosting_transactions.expires_at IS 'Дата истечения QR кода';


--
-- Name: invite_code_usage; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.invite_code_usage (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    invite_code_id uuid NOT NULL,
    user_id uuid NOT NULL,
    used_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.invite_code_usage OWNER TO appuser;

--
-- Name: TABLE invite_code_usage; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.invite_code_usage IS 'Tracks which users used which invite codes';


--
-- Name: invite_codes; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.invite_codes (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    code character varying(20) NOT NULL,
    group_id uuid,
    created_by uuid NOT NULL,
    max_uses integer,
    uses_count integer DEFAULT 0,
    expires_at timestamp without time zone,
    auto_approve boolean DEFAULT false,
    is_active boolean DEFAULT true,
    name character varying(100),
    notes text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    requires_registration boolean DEFAULT true,
    registration_data jsonb DEFAULT '{}'::jsonb,
    activated_at timestamp with time zone,
    activated_by uuid,
    target_role character varying(20) DEFAULT 'manager'::character varying,
    CONSTRAINT invite_codes_target_role_check CHECK (((target_role)::text = ANY ((ARRAY['manager'::character varying, 'supervisor'::character varying, 'director'::character varying])::text[])))
);


ALTER TABLE public.invite_codes OWNER TO appuser;

--
-- Name: TABLE invite_codes; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.invite_codes IS 'Invite codes for manager registration with group assignment';


--
-- Name: COLUMN invite_codes.code; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.invite_codes.code IS 'Unique invite code string, auto-generated uppercase alphanumeric';


--
-- Name: COLUMN invite_codes.group_id; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.invite_codes.group_id IS 'Admin group to auto-assign new managers to';


--
-- Name: COLUMN invite_codes.max_uses; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.invite_codes.max_uses IS 'Maximum number of registrations allowed, NULL for unlimited';


--
-- Name: COLUMN invite_codes.auto_approve; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.invite_codes.auto_approve IS 'If true, users registering with this code are auto-approved';


--
-- Name: COLUMN invite_codes.requires_registration; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.invite_codes.requires_registration IS 'Whether this invite requires full registration form';


--
-- Name: COLUMN invite_codes.activated_at; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.invite_codes.activated_at IS 'When the invite was first activated';


--
-- Name: COLUMN invite_codes.activated_by; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.invite_codes.activated_by IS 'User who activated this invite';


--
-- Name: COLUMN invite_codes.target_role; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.invite_codes.target_role IS 'The role to assign to users who register with this invite code';


--
-- Name: manager_settings; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.manager_settings (
    user_id uuid NOT NULL,
    is_blocked boolean DEFAULT false,
    block_reason text,
    blocked_at timestamp without time zone,
    blocked_by uuid,
    max_requests_per_day integer DEFAULT 50,
    max_active_requests integer DEFAULT 20,
    notes text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.manager_settings OWNER TO appuser;

--
-- Name: TABLE manager_settings; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.manager_settings IS 'Настройки и ограничения для менеджеров';


--
-- Name: pending_registrations; Type: VIEW; Schema: public; Owner: appuser
--

CREATE VIEW public.pending_registrations AS
 SELECT id,
    tg_id,
    first_name,
    last_name,
    contact,
    created_at,
    approval_status
   FROM public.users u
  WHERE (((approval_status)::text = 'pending'::text) AND (role = ANY (ARRAY['manager'::text, 'guest'::text])))
  ORDER BY created_at;


ALTER VIEW public.pending_registrations OWNER TO appuser;

--
-- Name: projects; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.projects (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    manager_id uuid,
    title text NOT NULL,
    status text DEFAULT 'draft'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    customer_id uuid,
    CONSTRAINT projects_status_check CHECK ((status = ANY (ARRAY['draft'::text, 'active'::text, 'onhold'::text, 'archived'::text])))
);


ALTER TABLE public.projects OWNER TO appuser;

--
-- Name: request_additional_services; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.request_additional_services (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    request_id uuid NOT NULL,
    service_id uuid NOT NULL,
    status character varying(30) DEFAULT 'pending'::character varying,
    notes text,
    added_by uuid,
    price character varying(100),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.request_additional_services OWNER TO appuser;

--
-- Name: TABLE request_additional_services; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.request_additional_services IS 'Связь заявок с дополнительными услугами';


--
-- Name: request_archive; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.request_archive (
    id uuid NOT NULL,
    original_request_id uuid NOT NULL,
    project_id uuid,
    manager_id uuid,
    payload_json jsonb,
    status character varying(30),
    result_url text,
    archive_reason character varying(50),
    archived_by uuid,
    archived_at timestamp without time zone DEFAULT now(),
    original_created_at timestamp without time zone,
    metadata jsonb DEFAULT '{}'::jsonb
);


ALTER TABLE public.request_archive OWNER TO appuser;

--
-- Name: TABLE request_archive; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.request_archive IS 'Архив завершённых/закрытых заявок';


--
-- Name: requests; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.requests (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    project_id uuid NOT NULL,
    payload_json jsonb NOT NULL,
    status text DEFAULT 'draft'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    generation_started_at timestamp without time zone,
    generation_completed_at timestamp without time zone,
    result_url text,
    error_message text,
    tariff character varying(20) DEFAULT 'standard'::character varying,
    chat_id bigint,
    CONSTRAINT requests_status_check CHECK ((status = ANY (ARRAY['draft'::text, 'awaiting_photos'::text, 'collecting_info'::text, 'collecting_photos'::text, 'ready_to_generate'::text, 'in_queue'::text, 'queued'::text, 'generating'::text, 'generated_ok'::text, 'generated_error'::text, 'success'::text, 'error'::text, 'archived'::text, 'closed'::text, 'cancelled'::text, 'delivered'::text]))),
    CONSTRAINT requests_tariff_check CHECK (((tariff)::text = ANY ((ARRAY['standard'::character varying, 'premium'::character varying])::text[])))
);


ALTER TABLE public.requests OWNER TO appuser;

--
-- Name: COLUMN requests.tariff; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.requests.tariff IS 'Generation tariff: standard (free) or premium (paid)';


--
-- Name: COLUMN requests.chat_id; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.requests.chat_id IS 'Telegram chat ID where request was created (for notifications)';


--
-- Name: CONSTRAINT requests_status_check ON requests; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON CONSTRAINT requests_status_check ON public.requests IS 'Допустимые статусы заявок';


--
-- Name: revision_history; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.revision_history (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    revision_id uuid NOT NULL,
    old_status character varying(50),
    new_status character varying(50) NOT NULL,
    changed_by uuid,
    change_source character varying(50) DEFAULT 'system'::character varying,
    comment text,
    metadata jsonb DEFAULT '{}'::jsonb,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.revision_history OWNER TO appuser;

--
-- Name: revision_stats; Type: VIEW; Schema: public; Owner: appuser
--

CREATE VIEW public.revision_stats AS
 SELECT cs.id AS site_id,
    cs.company_name,
    count(r.id) AS total_revisions,
    count(r.id) FILTER (WHERE ((r.status)::text = 'completed'::text)) AS completed_revisions,
    count(r.id) FILTER (WHERE ((r.status)::text = 'failed'::text)) AS failed_revisions,
    max(r.iteration) AS last_iteration,
    max(r.completed_at) AS last_revision_at
   FROM (public.client_sites cs
     LEFT JOIN public.revisions r ON ((r.site_id = cs.id)))
  GROUP BY cs.id, cs.company_name;


ALTER VIEW public.revision_stats OWNER TO appuser;

--
-- Name: VIEW revision_stats; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON VIEW public.revision_stats IS 'Статистика по правкам сайтов';


--
-- Name: service_categories; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.service_categories (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    parent_id uuid,
    name character varying(255) NOT NULL,
    description text,
    icon character varying(50),
    sort_order integer DEFAULT 0,
    is_active boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.service_categories OWNER TO appuser;

--
-- Name: TABLE service_categories; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.service_categories IS 'Hierarchical service categories for requests';


--
-- Name: site_editor_clients; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.site_editor_clients (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    site_id uuid NOT NULL,
    auth_user_id character varying(100),
    login character varying(100) NOT NULL,
    company_name character varying(255) NOT NULL,
    client_name character varying(255),
    client_contact character varying(255),
    telegram_id character varying(50),
    cms_site_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.site_editor_clients OWNER TO appuser;

--
-- Name: sites; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.sites (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    user_id uuid NOT NULL,
    request_id character varying(100) NOT NULL,
    domain character varying(255) NOT NULL,
    draft_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    live_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    base_archive_s3_key character varying(500)
);


ALTER TABLE public.sites OWNER TO appuser;

--
-- Name: COLUMN sites.base_archive_s3_key; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.sites.base_archive_s3_key IS 'S3 key (path) to the base .zip archive with React project sources. Example: requests/14b1ee9d.../Ki-ki_nail.zip';


--
-- Name: sites_stats; Type: VIEW; Schema: public; Owner: appuser
--

CREATE VIEW public.sites_stats AS
 SELECT count(*) AS total_sites,
    count(*) FILTER (WHERE ((deploy_status)::text = 'active'::text)) AS active_sites,
    count(*) FILTER (WHERE (((deploy_status)::text = 'pending'::text) OR ((deploy_status)::text = 'deploying'::text))) AS pending_sites,
    count(*) FILTER (WHERE ((deploy_status)::text = 'failed'::text)) AS failed_sites,
    count(*) FILTER (WHERE ((generation_status)::text = 'generating'::text)) AS generating_sites,
    count(*) FILTER (WHERE ((hosting_plan)::text = 'trial'::text)) AS trial_sites,
    count(*) FILTER (WHERE (((hosting_plan)::text <> 'trial'::text) AND (hosting_plan IS NOT NULL))) AS paid_sites,
    count(*) FILTER (WHERE (hosting_expires_at < now())) AS expired_sites
   FROM public.client_sites;


ALTER VIEW public.sites_stats OWNER TO appuser;

--
-- Name: system_settings; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.system_settings (
    key character varying(100) NOT NULL,
    value text,
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.system_settings OWNER TO appuser;

--
-- Name: top_managers; Type: VIEW; Schema: public; Owner: appuser
--

CREATE VIEW public.top_managers AS
 SELECT u.id,
    u.tg_id,
    u.first_name,
    u.last_name,
    u.contact,
    u.created_at AS registered_at,
    count(r.id) AS total_requests,
    count(r.id) FILTER (WHERE (r.status = 'generated_ok'::text)) AS completed,
    count(r.id) FILTER (WHERE (r.status = 'generated_error'::text)) AS failed,
    count(r.id) FILTER (WHERE (r.status = ANY (ARRAY['draft'::text, 'collecting_info'::text, 'collecting_photos'::text, 'ready_to_generate'::text, 'queued'::text]))) AS in_progress,
    COALESCE(ms.is_blocked, false) AS is_blocked
   FROM (((public.users u
     LEFT JOIN public.projects p ON ((p.manager_id = u.id)))
     LEFT JOIN public.requests r ON ((r.project_id = p.id)))
     LEFT JOIN public.manager_settings ms ON ((ms.user_id = u.id)))
  WHERE (u.role = 'manager'::text)
  GROUP BY u.id, u.tg_id, u.first_name, u.last_name, u.contact, u.created_at, ms.is_blocked
  ORDER BY (count(r.id)) DESC;


ALTER VIEW public.top_managers OWNER TO appuser;

--
-- Name: user_group_membership; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.user_group_membership (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    group_id uuid NOT NULL,
    role character varying(20) DEFAULT 'member'::character varying,
    added_by uuid,
    created_at timestamp without time zone DEFAULT now(),
    CONSTRAINT user_group_membership_role_check CHECK (((role)::text = ANY ((ARRAY['admin'::character varying, 'member'::character varying])::text[])))
);


ALTER TABLE public.user_group_membership OWNER TO appuser;

--
-- Name: TABLE user_group_membership; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.user_group_membership IS 'Many-to-many relationship between users and groups';


--
-- Name: versions; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.versions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    project_id uuid NOT NULL,
    number integer NOT NULL,
    prompt_json jsonb,
    sitemap_json jsonb,
    qa_report_json jsonb,
    artifact_key text,
    preview_url text,
    status text DEFAULT 'building'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT versions_status_check CHECK ((status = ANY (ARRAY['building'::text, 'ready'::text, 'failed'::text])))
);


ALTER TABLE public.versions OWNER TO appuser;

--
-- Name: webhook_events; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.webhook_events (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    source text NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT webhook_events_source_check CHECK ((source = ANY (ARRAY['tg'::text, 'n8n'::text, 'agent'::text])))
);


ALTER TABLE public.webhook_events OWNER TO appuser;

--
-- Name: workspace_resources; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.workspace_resources (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    workspace_id uuid NOT NULL,
    resource_type character varying(50) NOT NULL,
    resource_id uuid,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.workspace_resources OWNER TO appuser;

--
-- Name: workspaces; Type: TABLE; Schema: public; Owner: appuser
--

CREATE TABLE public.workspaces (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name character varying(255) NOT NULL,
    slug character varying(100) NOT NULL,
    owner_id uuid NOT NULL,
    settings jsonb DEFAULT '{}'::jsonb,
    max_requests integer DEFAULT 100,
    max_sites integer DEFAULT 10,
    max_storage_mb integer DEFAULT 1000,
    status character varying(50) DEFAULT 'active'::character varying,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT workspaces_status_check CHECK (((status)::text = ANY ((ARRAY['active'::character varying, 'suspended'::character varying, 'deleted'::character varying])::text[])))
);


ALTER TABLE public.workspaces OWNER TO appuser;

--
-- Name: TABLE workspaces; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON TABLE public.workspaces IS 'Tenant workspaces for manager isolation';


--
-- Name: COLUMN workspaces.slug; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.workspaces.slug IS 'URL-friendly unique identifier';


--
-- Name: COLUMN workspaces.settings; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.workspaces.settings IS 'Workspace-specific settings (JSON)';


--
-- Name: COLUMN workspaces.max_requests; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.workspaces.max_requests IS 'Maximum number of requests allowed';


--
-- Name: COLUMN workspaces.max_sites; Type: COMMENT; Schema: public; Owner: appuser
--

COMMENT ON COLUMN public.workspaces.max_sites IS 'Maximum number of deployed sites';


--
-- Name: admin_settings id; Type: DEFAULT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.admin_settings ALTER COLUMN id SET DEFAULT nextval('public.admin_settings_id_seq'::regclass);


--
-- Name: activity_log activity_log_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.activity_log
    ADD CONSTRAINT activity_log_pkey PRIMARY KEY (id);


--
-- Name: additional_services additional_services_code_key; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.additional_services
    ADD CONSTRAINT additional_services_code_key UNIQUE (code);


--
-- Name: additional_services additional_services_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.additional_services
    ADD CONSTRAINT additional_services_pkey PRIMARY KEY (id);


--
-- Name: admin_groups admin_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.admin_groups
    ADD CONSTRAINT admin_groups_pkey PRIMARY KEY (id);


--
-- Name: admin_notifications admin_notifications_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.admin_notifications
    ADD CONSTRAINT admin_notifications_pkey PRIMARY KEY (id);


--
-- Name: admin_settings admin_settings_key_key; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.admin_settings
    ADD CONSTRAINT admin_settings_key_key UNIQUE (key);


--
-- Name: admin_settings admin_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.admin_settings
    ADD CONSTRAINT admin_settings_pkey PRIMARY KEY (id);


--
-- Name: agent_runs agent_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.agent_runs
    ADD CONSTRAINT agent_runs_pkey PRIMARY KEY (id);


--
-- Name: anti_nuke_settings anti_nuke_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.anti_nuke_settings
    ADD CONSTRAINT anti_nuke_settings_pkey PRIMARY KEY (id);


--
-- Name: anti_nuke_settings anti_nuke_settings_setting_key_key; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.anti_nuke_settings
    ADD CONSTRAINT anti_nuke_settings_setting_key_key UNIQUE (setting_key);


--
-- Name: change_requests change_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.change_requests
    ADD CONSTRAINT change_requests_pkey PRIMARY KEY (id);


--
-- Name: client_sites client_sites_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.client_sites
    ADD CONSTRAINT client_sites_pkey PRIMARY KEY (id);


--
-- Name: current_request current_request_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.current_request
    ADD CONSTRAINT current_request_pkey PRIMARY KEY (tgid);


--
-- Name: daily_stats daily_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.daily_stats
    ADD CONSTRAINT daily_stats_pkey PRIMARY KEY (stat_date);


--
-- Name: deletion_audit_log deletion_audit_log_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.deletion_audit_log
    ADD CONSTRAINT deletion_audit_log_pkey PRIMARY KEY (id);


--
-- Name: deploy_history deploy_history_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.deploy_history
    ADD CONSTRAINT deploy_history_pkey PRIMARY KEY (id);


--
-- Name: files files_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_pkey PRIMARY KEY (id);


--
-- Name: hosting_plans hosting_plans_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.hosting_plans
    ADD CONSTRAINT hosting_plans_pkey PRIMARY KEY (id);


--
-- Name: hosting_transactions hosting_transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.hosting_transactions
    ADD CONSTRAINT hosting_transactions_pkey PRIMARY KEY (id);


--
-- Name: invite_code_usage invite_code_usage_invite_code_id_user_id_key; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.invite_code_usage
    ADD CONSTRAINT invite_code_usage_invite_code_id_user_id_key UNIQUE (invite_code_id, user_id);


--
-- Name: invite_code_usage invite_code_usage_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.invite_code_usage
    ADD CONSTRAINT invite_code_usage_pkey PRIMARY KEY (id);


--
-- Name: invite_codes invite_codes_code_key; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.invite_codes
    ADD CONSTRAINT invite_codes_code_key UNIQUE (code);


--
-- Name: invite_codes invite_codes_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.invite_codes
    ADD CONSTRAINT invite_codes_pkey PRIMARY KEY (id);


--
-- Name: manager_feedback manager_feedback_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.manager_feedback
    ADD CONSTRAINT manager_feedback_pkey PRIMARY KEY (id);


--
-- Name: manager_settings manager_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.manager_settings
    ADD CONSTRAINT manager_settings_pkey PRIMARY KEY (user_id);


--
-- Name: projects projects_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_pkey PRIMARY KEY (id);


--
-- Name: request_additional_services request_additional_services_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.request_additional_services
    ADD CONSTRAINT request_additional_services_pkey PRIMARY KEY (id);


--
-- Name: request_additional_services request_additional_services_request_id_service_id_key; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.request_additional_services
    ADD CONSTRAINT request_additional_services_request_id_service_id_key UNIQUE (request_id, service_id);


--
-- Name: request_archive request_archive_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.request_archive
    ADD CONSTRAINT request_archive_pkey PRIMARY KEY (id);


--
-- Name: requests requests_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.requests
    ADD CONSTRAINT requests_pkey PRIMARY KEY (id);


--
-- Name: revision_changes revision_changes_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.revision_changes
    ADD CONSTRAINT revision_changes_pkey PRIMARY KEY (id);


--
-- Name: revision_history revision_history_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.revision_history
    ADD CONSTRAINT revision_history_pkey PRIMARY KEY (id);


--
-- Name: revisions revisions_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.revisions
    ADD CONSTRAINT revisions_pkey PRIMARY KEY (id);


--
-- Name: service_categories service_categories_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.service_categories
    ADD CONSTRAINT service_categories_pkey PRIMARY KEY (id);


--
-- Name: site_editor_clients site_editor_clients_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.site_editor_clients
    ADD CONSTRAINT site_editor_clients_pkey PRIMARY KEY (id);


--
-- Name: sites sites_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.sites
    ADD CONSTRAINT sites_pkey PRIMARY KEY (id);


--
-- Name: sites sites_request_id_key; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.sites
    ADD CONSTRAINT sites_request_id_key UNIQUE (request_id);


--
-- Name: system_settings system_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.system_settings
    ADD CONSTRAINT system_settings_pkey PRIMARY KEY (key);


--
-- Name: site_editor_clients unique_site_editor_client; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.site_editor_clients
    ADD CONSTRAINT unique_site_editor_client UNIQUE (site_id);


--
-- Name: revisions unique_site_iteration; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.revisions
    ADD CONSTRAINT unique_site_iteration UNIQUE (site_id, iteration);


--
-- Name: user_group_membership user_group_membership_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.user_group_membership
    ADD CONSTRAINT user_group_membership_pkey PRIMARY KEY (id);


--
-- Name: user_group_membership user_group_membership_user_id_group_id_key; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.user_group_membership
    ADD CONSTRAINT user_group_membership_user_id_group_id_key UNIQUE (user_id, group_id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users users_tg_id_key; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_tg_id_key UNIQUE (tg_id);


--
-- Name: versions versions_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.versions
    ADD CONSTRAINT versions_pkey PRIMARY KEY (id);


--
-- Name: versions versions_project_id_number_key; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.versions
    ADD CONSTRAINT versions_project_id_number_key UNIQUE (project_id, number);


--
-- Name: webhook_events webhook_events_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.webhook_events
    ADD CONSTRAINT webhook_events_pkey PRIMARY KEY (id);


--
-- Name: workspace_resources workspace_resources_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.workspace_resources
    ADD CONSTRAINT workspace_resources_pkey PRIMARY KEY (id);


--
-- Name: workspaces workspaces_pkey; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.workspaces
    ADD CONSTRAINT workspaces_pkey PRIMARY KEY (id);


--
-- Name: workspaces workspaces_slug_key; Type: CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.workspaces
    ADD CONSTRAINT workspaces_slug_key UNIQUE (slug);


--
-- Name: idx_activity_log_action; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_activity_log_action ON public.activity_log USING btree (action);


--
-- Name: idx_activity_log_created; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_activity_log_created ON public.activity_log USING btree (created_at DESC);


--
-- Name: idx_activity_log_entity; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_activity_log_entity ON public.activity_log USING btree (entity_type, entity_id);


--
-- Name: idx_activity_log_user; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_activity_log_user ON public.activity_log USING btree (user_id);


--
-- Name: idx_admin_notifications_unread; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_admin_notifications_unread ON public.admin_notifications USING btree (is_read, created_at DESC);


--
-- Name: idx_admin_settings_key; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_admin_settings_key ON public.admin_settings USING btree (key);


--
-- Name: idx_agent_runs_project; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_agent_runs_project ON public.agent_runs USING btree (project_id);


--
-- Name: idx_agent_runs_type; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_agent_runs_type ON public.agent_runs USING btree (type);


--
-- Name: idx_change_requests_project; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_change_requests_project ON public.change_requests USING btree (project_id);


--
-- Name: idx_client_sites_cms_site_id; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_client_sites_cms_site_id ON public.client_sites USING btree (cms_site_id);


--
-- Name: idx_client_sites_deploy_id; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_client_sites_deploy_id ON public.client_sites USING btree (deploy_id);


--
-- Name: idx_client_sites_deploy_id_btree; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_client_sites_deploy_id_btree ON public.client_sites USING btree (deploy_id);


--
-- Name: idx_client_sites_deploy_status; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_client_sites_deploy_status ON public.client_sites USING btree (deploy_status);


--
-- Name: idx_client_sites_domain; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_client_sites_domain ON public.client_sites USING btree (domain);


--
-- Name: idx_client_sites_hosting_expires; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_client_sites_hosting_expires ON public.client_sites USING btree (hosting_expires_at);


--
-- Name: idx_client_sites_manager; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_client_sites_manager ON public.client_sites USING btree (manager_id);


--
-- Name: idx_client_sites_payment_warning; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_client_sites_payment_warning ON public.client_sites USING btree (payment_warning_sent_at);


--
-- Name: idx_client_sites_preview_slug; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_client_sites_preview_slug ON public.client_sites USING btree (preview_slug);


--
-- Name: idx_client_sites_request; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_client_sites_request ON public.client_sites USING btree (request_id);


--
-- Name: idx_client_sites_request_unique; Type: INDEX; Schema: public; Owner: appuser
--

CREATE UNIQUE INDEX idx_client_sites_request_unique ON public.client_sites USING btree (request_id);


--
-- Name: idx_client_sites_scheduled_deletion; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_client_sites_scheduled_deletion ON public.client_sites USING btree (scheduled_for_deletion_at);


--
-- Name: idx_current_request_request; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_current_request_request ON public.current_request USING btree (request_id);


--
-- Name: idx_deletion_audit_created; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_deletion_audit_created ON public.deletion_audit_log USING btree (created_at DESC);


--
-- Name: idx_deletion_audit_performer; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_deletion_audit_performer ON public.deletion_audit_log USING btree (performed_by);


--
-- Name: idx_deletion_audit_type; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_deletion_audit_type ON public.deletion_audit_log USING btree (action_type);


--
-- Name: idx_deploy_history_deploy_id; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_deploy_history_deploy_id ON public.deploy_history USING btree (deploy_id);


--
-- Name: idx_deploy_history_site; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_deploy_history_site ON public.deploy_history USING btree (client_site_id);


--
-- Name: idx_feedback_created; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_feedback_created ON public.manager_feedback USING btree (created_at DESC);


--
-- Name: idx_feedback_manager; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_feedback_manager ON public.manager_feedback USING btree (manager_id);


--
-- Name: idx_feedback_priority; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_feedback_priority ON public.manager_feedback USING btree (priority);


--
-- Name: idx_feedback_status; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_feedback_status ON public.manager_feedback USING btree (status);


--
-- Name: idx_files_project; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_files_project ON public.files USING btree (project_id);


--
-- Name: idx_files_version; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_files_version ON public.files USING btree (version_id);


--
-- Name: idx_hosting_transactions_created; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_hosting_transactions_created ON public.hosting_transactions USING btree (created_at DESC);


--
-- Name: idx_hosting_transactions_expires; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_hosting_transactions_expires ON public.hosting_transactions USING btree (expires_at);


--
-- Name: idx_hosting_transactions_site; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_hosting_transactions_site ON public.hosting_transactions USING btree (client_site_id);


--
-- Name: idx_hosting_transactions_status; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_hosting_transactions_status ON public.hosting_transactions USING btree (status);


--
-- Name: idx_invite_code_usage_code; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_invite_code_usage_code ON public.invite_code_usage USING btree (invite_code_id);


--
-- Name: idx_invite_code_usage_user; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_invite_code_usage_user ON public.invite_code_usage USING btree (user_id);


--
-- Name: idx_invite_codes_active; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_invite_codes_active ON public.invite_codes USING btree (is_active) WHERE (is_active = true);


--
-- Name: idx_invite_codes_code; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_invite_codes_code ON public.invite_codes USING btree (code);


--
-- Name: idx_invite_codes_created_by; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_invite_codes_created_by ON public.invite_codes USING btree (created_by);


--
-- Name: idx_invite_codes_group; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_invite_codes_group ON public.invite_codes USING btree (group_id);


--
-- Name: idx_invite_codes_group_id; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_invite_codes_group_id ON public.invite_codes USING btree (group_id);


--
-- Name: idx_invite_usage_code; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_invite_usage_code ON public.invite_code_usage USING btree (invite_code_id);


--
-- Name: idx_invite_usage_user; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_invite_usage_user ON public.invite_code_usage USING btree (user_id);


--
-- Name: idx_projects_customer; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_projects_customer ON public.projects USING btree (customer_id);


--
-- Name: idx_projects_manager; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_projects_manager ON public.projects USING btree (manager_id);


--
-- Name: idx_request_archive_date; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_request_archive_date ON public.request_archive USING btree (archived_at DESC);


--
-- Name: idx_request_archive_manager; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_request_archive_manager ON public.request_archive USING btree (manager_id);


--
-- Name: idx_request_archive_status; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_request_archive_status ON public.request_archive USING btree (status);


--
-- Name: idx_request_services_request; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_request_services_request ON public.request_additional_services USING btree (request_id);


--
-- Name: idx_request_services_service; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_request_services_service ON public.request_additional_services USING btree (service_id);


--
-- Name: idx_request_services_status; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_request_services_status ON public.request_additional_services USING btree (status);


--
-- Name: idx_requests_chat_id; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_requests_chat_id ON public.requests USING btree (chat_id);


--
-- Name: idx_requests_created; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_requests_created ON public.requests USING btree (created_at DESC);


--
-- Name: idx_requests_created_at; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_requests_created_at ON public.requests USING btree (created_at DESC);


--
-- Name: idx_requests_generation; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_requests_generation ON public.requests USING btree (generation_started_at, generation_completed_at);


--
-- Name: idx_requests_payload_gin; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_requests_payload_gin ON public.requests USING gin (payload_json);


--
-- Name: idx_requests_project; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_requests_project ON public.requests USING btree (project_id);


--
-- Name: idx_requests_status; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_requests_status ON public.requests USING btree (status);


--
-- Name: idx_requests_status_new; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_requests_status_new ON public.requests USING btree (status);


--
-- Name: idx_requests_tariff; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_requests_tariff ON public.requests USING btree (tariff);


--
-- Name: idx_revision_changes_revision_id; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_revision_changes_revision_id ON public.revision_changes USING btree (revision_id);


--
-- Name: idx_revision_changes_status; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_revision_changes_status ON public.revision_changes USING btree (status);


--
-- Name: idx_revision_changes_type; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_revision_changes_type ON public.revision_changes USING btree (change_type);


--
-- Name: idx_revision_history_revision_id; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_revision_history_revision_id ON public.revision_history USING btree (revision_id);


--
-- Name: idx_revisions_created_at; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_revisions_created_at ON public.revisions USING btree (created_at DESC);


--
-- Name: idx_revisions_n8n_job_id; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_revisions_n8n_job_id ON public.revisions USING btree (n8n_job_id);


--
-- Name: idx_revisions_site_id; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_revisions_site_id ON public.revisions USING btree (site_id);


--
-- Name: idx_revisions_status; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_revisions_status ON public.revisions USING btree (status);


--
-- Name: idx_service_categories_parent; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_service_categories_parent ON public.service_categories USING btree (parent_id);


--
-- Name: idx_site_editor_clients_auth_user; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_site_editor_clients_auth_user ON public.site_editor_clients USING btree (auth_user_id);


--
-- Name: idx_site_editor_clients_login; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_site_editor_clients_login ON public.site_editor_clients USING btree (login);


--
-- Name: idx_sites_domain; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_sites_domain ON public.sites USING btree (domain);


--
-- Name: idx_sites_request_id; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_sites_request_id ON public.sites USING btree (request_id);


--
-- Name: idx_sites_user_id; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_sites_user_id ON public.sites USING btree (user_id);


--
-- Name: idx_user_group_group; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_user_group_group ON public.user_group_membership USING btree (group_id);


--
-- Name: idx_user_group_user; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_user_group_user ON public.user_group_membership USING btree (user_id);


--
-- Name: idx_users_approval; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_users_approval ON public.users USING btree (approval_status);


--
-- Name: idx_users_email; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_users_email ON public.users USING btree (email) WHERE (email IS NOT NULL);


--
-- Name: idx_users_is_blocked; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_users_is_blocked ON public.users USING btree (is_blocked) WHERE (is_blocked = true);


--
-- Name: idx_users_phone; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_users_phone ON public.users USING btree (phone) WHERE (phone IS NOT NULL);


--
-- Name: idx_users_registered_via; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_users_registered_via ON public.users USING btree (registered_via_code) WHERE (registered_via_code IS NOT NULL);


--
-- Name: idx_users_username; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_users_username ON public.users USING btree (username);


--
-- Name: idx_users_workspace; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_users_workspace ON public.users USING btree (workspace_id);


--
-- Name: idx_versions_project; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_versions_project ON public.versions USING btree (project_id);


--
-- Name: idx_webhook_events_payload_gin; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_webhook_events_payload_gin ON public.webhook_events USING gin (payload);


--
-- Name: idx_webhook_events_source; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_webhook_events_source ON public.webhook_events USING btree (source);


--
-- Name: idx_workspace_resources_type; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_workspace_resources_type ON public.workspace_resources USING btree (workspace_id, resource_type);


--
-- Name: idx_workspace_resources_workspace; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_workspace_resources_workspace ON public.workspace_resources USING btree (workspace_id);


--
-- Name: idx_workspaces_owner; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_workspaces_owner ON public.workspaces USING btree (owner_id);


--
-- Name: idx_workspaces_slug; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_workspaces_slug ON public.workspaces USING btree (slug);


--
-- Name: idx_workspaces_status; Type: INDEX; Schema: public; Owner: appuser
--

CREATE INDEX idx_workspaces_status ON public.workspaces USING btree (status);


--
-- Name: admin_groups set_timestamp_admin_groups; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER set_timestamp_admin_groups BEFORE UPDATE ON public.admin_groups FOR EACH ROW EXECUTE FUNCTION public.trigger_set_timestamp();


--
-- Name: invite_codes set_timestamp_invite_codes; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER set_timestamp_invite_codes BEFORE UPDATE ON public.invite_codes FOR EACH ROW EXECUTE FUNCTION public.trigger_set_timestamp();


--
-- Name: service_categories set_timestamp_service_categories; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER set_timestamp_service_categories BEFORE UPDATE ON public.service_categories FOR EACH ROW EXECUTE FUNCTION public.trigger_set_timestamp();


--
-- Name: revision_changes tr_revision_changes_updated_at; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER tr_revision_changes_updated_at BEFORE UPDATE ON public.revision_changes FOR EACH ROW EXECUTE FUNCTION public.update_revision_updated_at();


--
-- Name: revisions tr_revisions_updated_at; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER tr_revisions_updated_at BEFORE UPDATE ON public.revisions FOR EACH ROW EXECUTE FUNCTION public.update_revision_updated_at();


--
-- Name: revisions tr_update_site_revision_count; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER tr_update_site_revision_count AFTER INSERT OR UPDATE ON public.revisions FOR EACH ROW EXECUTE FUNCTION public.update_site_revision_count();


--
-- Name: projects trg_projects_updated; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER trg_projects_updated BEFORE UPDATE ON public.projects FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();


--
-- Name: requests trg_requests_updated; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER trg_requests_updated BEFORE UPDATE ON public.requests FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();


--
-- Name: invite_codes trigger_invite_codes_updated_at; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER trigger_invite_codes_updated_at BEFORE UPDATE ON public.invite_codes FOR EACH ROW EXECUTE FUNCTION public.update_invite_codes_updated_at();


--
-- Name: workspaces trigger_workspaces_updated_at; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER trigger_workspaces_updated_at BEFORE UPDATE ON public.workspaces FOR EACH ROW EXECUTE FUNCTION public.update_workspaces_updated_at();


--
-- Name: additional_services update_additional_services_updated_at; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER update_additional_services_updated_at BEFORE UPDATE ON public.additional_services FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: manager_feedback update_manager_feedback_updated_at; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER update_manager_feedback_updated_at BEFORE UPDATE ON public.manager_feedback FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: manager_settings update_manager_settings_updated_at; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER update_manager_settings_updated_at BEFORE UPDATE ON public.manager_settings FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: request_additional_services update_request_services_updated_at; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER update_request_services_updated_at BEFORE UPDATE ON public.request_additional_services FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: sites update_sites_updated_at; Type: TRIGGER; Schema: public; Owner: appuser
--

CREATE TRIGGER update_sites_updated_at BEFORE UPDATE ON public.sites FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


--
-- Name: activity_log activity_log_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.activity_log
    ADD CONSTRAINT activity_log_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: admin_groups admin_groups_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.admin_groups
    ADD CONSTRAINT admin_groups_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: agent_runs agent_runs_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.agent_runs
    ADD CONSTRAINT agent_runs_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;


--
-- Name: agent_runs agent_runs_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.agent_runs
    ADD CONSTRAINT agent_runs_version_id_fkey FOREIGN KEY (version_id) REFERENCES public.versions(id) ON DELETE SET NULL;


--
-- Name: anti_nuke_settings anti_nuke_settings_updated_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.anti_nuke_settings
    ADD CONSTRAINT anti_nuke_settings_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: change_requests change_requests_base_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.change_requests
    ADD CONSTRAINT change_requests_base_version_id_fkey FOREIGN KEY (base_version_id) REFERENCES public.versions(id) ON DELETE SET NULL;


--
-- Name: change_requests change_requests_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.change_requests
    ADD CONSTRAINT change_requests_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;


--
-- Name: client_sites client_sites_current_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.client_sites
    ADD CONSTRAINT client_sites_current_revision_id_fkey FOREIGN KEY (current_revision_id) REFERENCES public.revisions(id) ON DELETE SET NULL;


--
-- Name: client_sites client_sites_manager_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.client_sites
    ADD CONSTRAINT client_sites_manager_id_fkey FOREIGN KEY (manager_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: client_sites client_sites_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.client_sites
    ADD CONSTRAINT client_sites_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.requests(id) ON DELETE SET NULL;


--
-- Name: current_request current_request_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.current_request
    ADD CONSTRAINT current_request_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.requests(id) ON DELETE CASCADE;


--
-- Name: deletion_audit_log deletion_audit_log_performed_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.deletion_audit_log
    ADD CONSTRAINT deletion_audit_log_performed_by_fkey FOREIGN KEY (performed_by) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: deploy_history deploy_history_client_site_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.deploy_history
    ADD CONSTRAINT deploy_history_client_site_id_fkey FOREIGN KEY (client_site_id) REFERENCES public.client_sites(id) ON DELETE CASCADE;


--
-- Name: deploy_history deploy_history_initiated_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.deploy_history
    ADD CONSTRAINT deploy_history_initiated_by_fkey FOREIGN KEY (initiated_by) REFERENCES public.users(id);


--
-- Name: files files_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;


--
-- Name: files files_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_version_id_fkey FOREIGN KEY (version_id) REFERENCES public.versions(id) ON DELETE SET NULL;


--
-- Name: hosting_transactions hosting_transactions_client_site_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.hosting_transactions
    ADD CONSTRAINT hosting_transactions_client_site_id_fkey FOREIGN KEY (client_site_id) REFERENCES public.client_sites(id) ON DELETE CASCADE;


--
-- Name: hosting_transactions hosting_transactions_plan_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.hosting_transactions
    ADD CONSTRAINT hosting_transactions_plan_id_fkey FOREIGN KEY (plan_id) REFERENCES public.hosting_plans(id);


--
-- Name: invite_code_usage invite_code_usage_invite_code_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.invite_code_usage
    ADD CONSTRAINT invite_code_usage_invite_code_id_fkey FOREIGN KEY (invite_code_id) REFERENCES public.invite_codes(id) ON DELETE CASCADE;


--
-- Name: invite_code_usage invite_code_usage_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.invite_code_usage
    ADD CONSTRAINT invite_code_usage_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: invite_codes invite_codes_activated_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.invite_codes
    ADD CONSTRAINT invite_codes_activated_by_fkey FOREIGN KEY (activated_by) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: invite_codes invite_codes_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.invite_codes
    ADD CONSTRAINT invite_codes_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: invite_codes invite_codes_group_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.invite_codes
    ADD CONSTRAINT invite_codes_group_id_fkey FOREIGN KEY (group_id) REFERENCES public.admin_groups(id) ON DELETE CASCADE;


--
-- Name: manager_feedback manager_feedback_manager_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.manager_feedback
    ADD CONSTRAINT manager_feedback_manager_id_fkey FOREIGN KEY (manager_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: manager_feedback manager_feedback_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.manager_feedback
    ADD CONSTRAINT manager_feedback_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.requests(id) ON DELETE SET NULL;


--
-- Name: manager_feedback manager_feedback_responded_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.manager_feedback
    ADD CONSTRAINT manager_feedback_responded_by_fkey FOREIGN KEY (responded_by) REFERENCES public.users(id);


--
-- Name: manager_settings manager_settings_blocked_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.manager_settings
    ADD CONSTRAINT manager_settings_blocked_by_fkey FOREIGN KEY (blocked_by) REFERENCES public.users(id);


--
-- Name: manager_settings manager_settings_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.manager_settings
    ADD CONSTRAINT manager_settings_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: projects projects_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: projects projects_manager_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_manager_id_fkey FOREIGN KEY (manager_id) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: request_additional_services request_additional_services_added_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.request_additional_services
    ADD CONSTRAINT request_additional_services_added_by_fkey FOREIGN KEY (added_by) REFERENCES public.users(id);


--
-- Name: request_additional_services request_additional_services_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.request_additional_services
    ADD CONSTRAINT request_additional_services_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.requests(id) ON DELETE CASCADE;


--
-- Name: request_additional_services request_additional_services_service_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.request_additional_services
    ADD CONSTRAINT request_additional_services_service_id_fkey FOREIGN KEY (service_id) REFERENCES public.additional_services(id) ON DELETE CASCADE;


--
-- Name: request_archive request_archive_archived_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.request_archive
    ADD CONSTRAINT request_archive_archived_by_fkey FOREIGN KEY (archived_by) REFERENCES public.users(id);


--
-- Name: requests requests_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.requests
    ADD CONSTRAINT requests_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;


--
-- Name: revision_changes revision_changes_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.revision_changes
    ADD CONSTRAINT revision_changes_revision_id_fkey FOREIGN KEY (revision_id) REFERENCES public.revisions(id) ON DELETE CASCADE;


--
-- Name: revision_history revision_history_changed_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.revision_history
    ADD CONSTRAINT revision_history_changed_by_fkey FOREIGN KEY (changed_by) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: revision_history revision_history_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.revision_history
    ADD CONSTRAINT revision_history_revision_id_fkey FOREIGN KEY (revision_id) REFERENCES public.revisions(id) ON DELETE CASCADE;


--
-- Name: revisions revisions_manager_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.revisions
    ADD CONSTRAINT revisions_manager_id_fkey FOREIGN KEY (manager_id) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: revisions revisions_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.revisions
    ADD CONSTRAINT revisions_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.requests(id) ON DELETE SET NULL;


--
-- Name: revisions revisions_site_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.revisions
    ADD CONSTRAINT revisions_site_id_fkey FOREIGN KEY (site_id) REFERENCES public.client_sites(id) ON DELETE CASCADE;


--
-- Name: service_categories service_categories_parent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.service_categories
    ADD CONSTRAINT service_categories_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.service_categories(id) ON DELETE CASCADE;


--
-- Name: site_editor_clients site_editor_clients_site_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.site_editor_clients
    ADD CONSTRAINT site_editor_clients_site_id_fkey FOREIGN KEY (site_id) REFERENCES public.client_sites(id) ON DELETE CASCADE;


--
-- Name: user_group_membership user_group_membership_added_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.user_group_membership
    ADD CONSTRAINT user_group_membership_added_by_fkey FOREIGN KEY (added_by) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: user_group_membership user_group_membership_group_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.user_group_membership
    ADD CONSTRAINT user_group_membership_group_id_fkey FOREIGN KEY (group_id) REFERENCES public.admin_groups(id) ON DELETE CASCADE;


--
-- Name: user_group_membership user_group_membership_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.user_group_membership
    ADD CONSTRAINT user_group_membership_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: users users_admin_group_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_admin_group_id_fkey FOREIGN KEY (admin_group_id) REFERENCES public.admin_groups(id) ON DELETE SET NULL;


--
-- Name: users users_approved_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_approved_by_fkey FOREIGN KEY (approved_by) REFERENCES public.users(id);


--
-- Name: users users_registered_via_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_registered_via_code_fkey FOREIGN KEY (registered_via_code) REFERENCES public.invite_codes(id) ON DELETE SET NULL;


--
-- Name: users users_workspace_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_workspace_id_fkey FOREIGN KEY (workspace_id) REFERENCES public.workspaces(id) ON DELETE SET NULL;


--
-- Name: versions versions_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.versions
    ADD CONSTRAINT versions_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;


--
-- Name: workspace_resources workspace_resources_workspace_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.workspace_resources
    ADD CONSTRAINT workspace_resources_workspace_id_fkey FOREIGN KEY (workspace_id) REFERENCES public.workspaces(id) ON DELETE CASCADE;


--
-- Name: workspaces workspaces_owner_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: appuser
--

ALTER TABLE ONLY public.workspaces
    ADD CONSTRAINT workspaces_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict TBbh1GTUaxts1WbASsjAEqYWuA7hiqPkynbZoJ2RKnPyQndlHa0mBi869v1Pj7z

