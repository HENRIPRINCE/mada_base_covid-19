-- //------------------------------------------------02 databases
CREATE DATABASE bd_covid19
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'fr_FR.UTF-8'
    LC_CTYPE = 'fr_FR.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

CREATE DATABASE bd_reference
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'fr_FR.UTF-8'
    LC_CTYPE = 'fr_FR.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

-- //------------------------------------------------all tables in bd_reference
CREATE TABLE public.tbl_ref
(
    adm0_en character varying(255),
    adm0_pcode character varying(255),
    adm1_en character varying(255),
    adm1_pcode character varying(255),
    adm2_en character varying(255),
    adm2_pcode character varying(255),
    adm3_en character varying(255),
    adm3_pcode character varying(255),
    adm4_en character varying(255),
    adm4_pcode character varying(255),
    mdg_adm1_pcode character varying(255),
    mdg_adm2_pcode character varying(255),
    mdg_adm3_pcode character varying(255),
    mdg_adm4_pcode character varying(255),
    ocha_code_postal character varying(255)
);


CREATE TABLE public.tbl_ref_loc
(
    adm1_en character varying(255),
    adm2_en character varying(255),
    adm3_en character varying(255),
    adm4_en character varying(255),
    localite character varying(255),
    dis_class character varying(3),
    class_adm character varying(255),
    x_loc_ddm double precision,
    y_loc_ddm double precision,
    mdg_adm1_pcode character varying(255),
    mdg_adm2_pcode character varying(255),
    mdg_adm3_pcode character varying(255),
    mdg_adm4_pcode character varying(255),
    mdg_loc_code character varying(255)
);

CREATE TABLE public.view_ref_fs
(
    fs_nom character varying(254),
    fs_category character varying(254),
    fs_genre character varying(254),
    fs_service character varying(254),
    fs_source character varying(254),
	mdg_fkt_code character varying(254),
	coor_x numeric,
    coor_y numeric,
	fs_nom_total character varying(254)
);


--//------------------------grant
GRANT SELECT ON TABLE public.tbl_ref TO PG_ROLE;
GRANT SELECT ON TABLE public.tbl_ref_loc TO PG_ROLE;
GRANT SELECT ON TABLE public.view_ref_fs TO PG_ROLE;




-- //------------------------------------------------all tables in bd_covid
CREATE TABLE public.tbl_brute
(
    num_viro_interne text,
    region text,
    district text,
    commune text,
    adresse text,
    fsanitaire text,
    fsanitaire_hospital text,
    fsanitaire_declare text,
    date_arrive_prel_ipm date,
    date_update_brute timestamp with time zone
);

CREATE TABLE public.tbl_covid_ocha
(
    num_viro_interne character varying(255),
    region character varying(255),
    district character varying(255),
    commune character varying(255),
    adresse character varying(255),
    date_update_ocha character varying(255),
    mdg_reg_code character varying(255),
    mdg_dis_code character varying(255),
    mdg_com_code character varying(255),
    mdg_fkt_code character varying(255),
    niveau_nettoyage character varying(255),
    niveau_nettoyage_fs character varying(100),
    observations text,
    fsanitaire character varying(255),
    fsanitaire_hospital character varying(255),
    fsanitaire_declare character varying(255),
    ocha_fsanitaire character varying(255),
    ocha_fsanitaire_hospital character varying(255),
    ocha_fsanitaire_declare character varying(255),
    mdg_fkt_code_fsanitaire character varying(50),
    mdg_fkt_code_fsanitaire_hospital character varying(255),
    mdg_fkt_code_fsanitaire_declare character varying(255),
    date_arrive_prel_ipm character varying(255)
);

CREATE TABLE public.tbl_error_covid_adm
(
    region character varying(255),
    district character varying(255),
    commune character varying(255),
    mdg_adm1_pcode character varying(255),
    mdg_adm2_pcode character varying(255),
    mdg_adm3_pcode character varying(255),
    date_upload timestamp with time zone,
    from_to character varying(100)
);

CREATE TABLE public.tbl_error_covid_fs
(
    fsanitaire character varying(255),
    mdg_fkt_code character varying(255),
    ocha_fsanitaire character varying(255),
    date_upload timestamp with time zone
);

CREATE TABLE public.tbl_regex_fs
(
    ocha_fsanitaire character varying(255),
    mdg_fkt_code character varying(255),
    regex_fld character varying(255)
);

CREATE TABLE public.tbl_ocha_fsanitaire_corrige
(
    ocha_fsanitaire character varying(255),
    mdg_fkt_code character varying(255),
    date_upload character varying(255)
);

--//------------------------grant
GRANT INSERT, UPDATE, DELETE, SELECT ON TABLE public.tbl_covid_ocha TO PG_ROLE;
GRANT INSERT, UPDATE, DELETE, SELECT ON TABLE public.tbl_error_covid_adm TO PG_ROLE;
GRANT INSERT, UPDATE, DELETE, SELECT ON TABLE public.tbl_error_covid_fs TO PG_ROLE;
GRANT INSERT, UPDATE, DELETE, SELECT ON TABLE public.tbl_regex_fs TO PG_ROLE;
GRANT INSERT, UPDATE, DELETE, SELECT ON TABLE public.tbl_ocha_fsanitaire_corrige TO PG_ROLE;


-- //------------------------------------------------all tables in bd_covid for view
CREATE TABLE public.tbl_covid_cas
(
    num_viro_interne text,
    lieu_confinement text,
    age double precision,
    sexe text,
    date_saisi_res text,
    date_saisie text,
    statut_cas text,
    statut_final text,
    origine_cas text,
    symptome text,
    date_update timestamp with time zone
);


CREATE TABLE public.data_ocha_geom_22regions
(
    region text,
    adm1_pcode character varying(50),
    mdg_reg_code text,
    centr_x double precision,
    centr_y double precision
);
CREATE TABLE public.data_ocha_geom_119districts
(
    region text,
    district text,
    adm2_pcode character varying(50),
    mdg_dis_code text,
    centr_x double precision,
    centr_y double precision
);
CREATE TABLE public.data_ocha_geom_114districts
(
    region character varying(255),
    district character varying(255),
    mdg_reg_code character varying(30),
    mdg_dis_code character varying(30),
    centr_x double precision,
    centr_y double precision
);
CREATE TABLE public.data_ocha_geom_allcommunes
(
    region text,
    district text,
    commune text,
    adm2_pcode character varying(50),
    mdg_dis_code text,
    mdg_com_code text,
    centr_x double precision,
    centr_y double precision
);
CREATE TABLE public.data_ocha_geom_allfokontany
(
    region character varying(255),
    district character varying(255),
    commune character varying(255),
    fokontany character varying(255),
    adm4_pcode character varying(255),
    mdg_reg_code character varying(255),
    mdg_dis_code character varying(255),
    mdg_com_code character varying(255),
    mdg_fkt_code character varying(255),
    ocha_code_postal double precision,
    dis_class character varying(3)
);
--//------------------------grant
GRANT INSERT, DELETE, UPDATE, SELECT ON TABLE public.tbl_covid_cas TO pers_carte;
GRANT  SELECT ON TABLE public.data_ocha_geom_22regions TO PG_ROLE;
GRANT  SELECT ON TABLE public.data_ocha_geom_119districts TO PG_ROLE;
GRANT  SELECT ON TABLE public.data_ocha_geom_114districts TO PG_ROLE;
GRANT  SELECT ON TABLE public.data_ocha_geom_allcommunes TO PG_ROLE;
GRANT  SELECT ON TABLE public.data_ocha_geom_allfokontany TO PG_ROLE;

