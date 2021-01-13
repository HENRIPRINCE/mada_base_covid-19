CREATE OR REPLACE VIEW public.view_covid19_ocha
 AS
 SELECT tbl_covid_ocha.num_viro_interne,
    tbl_covid_ocha.region AS brute_region,
    tbl_covid_ocha.district AS brute_district,
    tbl_covid_ocha.district AS brute_commune,
    tbl_covid_ocha.adresse,
    tbl_covid_ocha.date_arrive_prel_ipm,
    tbl_covid_ocha.date_update_ocha,
    tbl_covid_ocha.fsanitaire,
    tbl_covid_ocha.fsanitaire_hospital,
    tbl_covid_ocha.fsanitaire_declare,
    tbl_covid_ocha.niveau_nettoyage,
    tbl_covid_ocha.niveau_nettoyage_fs,
    tbl_covid_ocha.mdg_reg_code,
    tbl_covid_ocha.mdg_dis_code,
    tbl_covid_ocha.mdg_com_code,
    tbl_covid_ocha.mdg_fkt_code,
    tbl_covid_ocha.mdg_fkt_code_fsanitaire,
    tbl_covid_ocha.mdg_fkt_code_fsanitaire_hospital,
    tbl_covid_ocha.mdg_fkt_code_fsanitaire_declare,
    tbl_covid_ocha.ocha_fsanitaire,
    tbl_covid_ocha.ocha_fsanitaire_hospital,
    tbl_covid_ocha.ocha_fsanitaire_declare,
    ( SELECT kondrana1r.region
           FROM ( SELECT data_ocha_geom_22regions.region,
                    data_ocha_geom_22regions.mdg_reg_code
                   FROM data_ocha_geom_22regions
                  WHERE data_ocha_geom_22regions.mdg_reg_code = tbl_covid_ocha.mdg_reg_code) kondrana1r) AS region,
    ( SELECT kondrana1d.district
           FROM ( SELECT data_ocha_geom_119districts.district,
                    data_ocha_geom_119districts.mdg_dis_code
                   FROM data_ocha_geom_119districts
                  WHERE data_ocha_geom_119districts.mdg_dis_code = tbl_covid_ocha.mdg_dis_code) kondrana1d) AS district,
    ( SELECT kondrana1c.commune
           FROM ( SELECT data_ocha_geom_allcommunes.commune,
                    data_ocha_geom_allcommunes.mdg_com_code
                   FROM data_ocha_geom_allcommunes
                  WHERE data_ocha_geom_allcommunes.mdg_com_code = tbl_covid_ocha.mdg_com_code) kondrana1c) AS commune,
    ( SELECT kondranafc.fokontany
           FROM ( SELECT data_ocha_geom_allfokontany.fokontany,
                    data_ocha_geom_allfokontany.mdg_fkt_code
                   FROM data_ocha_geom_allfokontany
                  WHERE data_ocha_geom_allfokontany.mdg_fkt_code = tbl_covid_ocha.mdg_fkt_code) kondranafc) AS fokontany,
    ( SELECT kondrana1.region
           FROM ( SELECT data_ocha_geom_114districts.region,
                    data_ocha_geom_114districts.mdg_dis_code
                   FROM data_ocha_geom_114districts
                  WHERE data_ocha_geom_114districts.mdg_dis_code = substring(tbl_covid_ocha.mdg_fkt_code_fsanitaire, 1, 8)) kondrana1) AS ocha_fsanitaire_region,
    ( SELECT kondrana2.region
           FROM ( SELECT data_ocha_geom_114districts.region,
                    data_ocha_geom_114districts.mdg_dis_code
                   FROM data_ocha_geom_114districts
                  WHERE data_ocha_geom_114districts.mdg_dis_code = substring(tbl_covid_ocha.mdg_fkt_code_fsanitaire_hospital, 1, 8)) kondrana2) AS ocha_fsanitaire_hospital_region,
    ( SELECT kondrana3.region
           FROM ( SELECT data_ocha_geom_114districts.region,
                    data_ocha_geom_114districts.mdg_dis_code
                   FROM data_ocha_geom_114districts
                  WHERE data_ocha_geom_114districts.mdg_dis_code = substring(tbl_covid_ocha.mdg_fkt_code_fsanitaire_declare, 1, 8)) kondrana3) AS ocha_fsanitaire_declare_region,
    ( SELECT kondrana4.district
           FROM ( SELECT data_ocha_geom_114districts.district,
                    data_ocha_geom_114districts.mdg_dis_code
                   FROM data_ocha_geom_114districts
                  WHERE data_ocha_geom_114districts.mdg_dis_code = substring(tbl_covid_ocha.mdg_fkt_code_fsanitaire, 1, 8)) kondrana4) AS ocha_fsanitaire_district,
    ( SELECT kondrana5.district
           FROM ( SELECT data_ocha_geom_114districts.district,
                    data_ocha_geom_114districts.mdg_dis_code
                   FROM data_ocha_geom_114districts
                  WHERE data_ocha_geom_114districts.mdg_dis_code = substring(tbl_covid_ocha.mdg_fkt_code_fsanitaire_hospital, 1, 8)) kondrana5) AS ocha_fsanitaire_hospital_district,
    ( SELECT kondrana6.district
           FROM ( SELECT data_ocha_geom_114districts.district,
                    data_ocha_geom_114districts.mdg_dis_code
                   FROM data_ocha_geom_114districts
                  WHERE data_ocha_geom_114districts.mdg_dis_code = substring(tbl_covid_ocha.mdg_fkt_code_fsanitaire_declare, 1, 8)) kondrana6) AS ocha_fsanitaire_declare_district,
    tbl_covid_cas.lieu_confinement,
    tbl_covid_cas.age,
    tbl_covid_cas.sexe,
    tbl_covid_cas.symptome,
    tbl_covid_cas.date_saisi_res,
    tbl_covid_cas.date_saisie,
    tbl_covid_cas.statut_cas,
    tbl_covid_cas.statut_final,
    tbl_covid_cas.origine_cas,
    tbl_covid_cas.date_update
   FROM tbl_covid_ocha
     LEFT JOIN tbl_covid_cas ON tbl_covid_ocha.num_viro_interne = tbl_covid_cas.num_viro_interne
  ORDER BY tbl_covid_ocha.num_viro_interne;

GRANT SELECT ON TABLE public.view_covid19_ocha TO PG_ROLE;

