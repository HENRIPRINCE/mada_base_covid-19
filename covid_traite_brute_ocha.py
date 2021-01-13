#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Crée par RAHOILIJAONA BIENVENUE
henriprincetoky@gmail.com
"""

import sys
import os
import os.path
import socket
import time
import re
import json
import psycopg2
#import requests
#from psycopg2.extras import RealDictCursor
import datetime
#from datetime import datetime
#from operator import and_
#from functools import reduce
#from time import sleep
#from datetime import timedelta
#from pathlib import Path

path_data_tmp = os.path.dirname(os.path.realpath(__file__)) + "/log_covid"
socket.setdefaulttimeout(15)
isa_testNet = 0
var_reg=""
var_dis=""
var_com=""
var_fs_null=""
var_fs_trouve=""
var_fs_localise=""
var_fs_groupe=""

class Adala():
    def __init__(self):
        self.sig_host = "PG_HOST"
        self.sig_port = "PG_PORTS"
        self.role_cnx = "PG_ROLE"
        self.mdp_cnx = "PG_MDP"
        self.bd_ref = "bd_reference"
        self.bd_covid = "bd_covid19"
        self.con_covid = psycopg2.connect("dbname='" + self.bd_covid + "' user='" + self.role_cnx + "' password='" + self.mdp_cnx + "' host='" + self.sig_host + "' port='"+ self.sig_port +"'")
        self.con_stat = psycopg2.connect("dbname='" + self.bd_ref + "' user='" + self.role_cnx + "' password='" + self.mdp_cnx + "' host='" + self.sig_host + "' port='"+ self.sig_port +"'")
   
        self.tbl_ref ="public.tbl_ref"
        self.tbl_ref_loc="public.tbl_ref_loc"
        self.view_ref_fs ="public.view_ref_fs"
        		
        self.tbl_brute ="public.tbl_brute"
        self.tbl_covid_ocha ="public.tbl_covid_ocha"
        self.tbl_error_covid_adm ="public.tbl_error_covid_adm"
        self.tbl_error_covid_fs ="public.tbl_error_covid_fs"
        self.tbl_regex_fs = "public.tbl_regex_fs"
        self.tbl_ocha_fsanitaire_corrige ="public.tbl_ocha_fsanitaire_corrige"
        self.nom_log = "LOG_COVID_" + datetime.now().strftime('%Y%m%d_%H%M') + ".txt"
        self.dir_log = path_data_tmp +"/" + self.nom_log
        self.isa_averina = 0
        self.isa_niv_reg =0
        self.isa_niv_dis =0
        self.isa_niv_com =0
        self.isa_niv_reg_2 =0
        self.isa_niv_dis_2 =0
        self.new_enreg = 0
        self.norme_fs = 0
        self.error_fs = 0
        self.nbre_reg_trouve = 0
        self.nbre_dis_trouve = 0
        self.nbre_com_trouve = 0
        self.nbre_fs_trouve = 0
        self.nbre_fs_localise = 0
        self.nbre_fs_groupe = 0
        self.nbre_fs_null = 0
        self.nbre_maj_fs = 0
        self.nbre_maj_reg =0
        self.nbre_maj_dis =0
        self.nbre_maj_com =0

        #self.error_dm = ["---", "--", "-", "None", "Manquant", "Manquante", "NA", '']
        self.error_dm = ["None", "NONE", "Manquant", "Manquante","NA",'']
            
        
    def jsonDefault(self, object):
        return object.__dict__

    def len_daty(self, volana):
        if volana < 10:
            v_back = "0" + str(volana)
        else:
            v_back = str(volana)
        return v_back
    
    def misysokatra(self, sokatra):
        return bool(re.search(r'\d', sokatra))
    
    
    def alana_chiffres(self, teny):
        nom_vao = ""
        for x in teny:
            if not x.isdigit() and x != "-":
                nom_vao = nom_vao + x
    
    def SLike_objet(self, xvalue, xchamps):
        sHaving = ""
        if xvalue is not None:
            for xi in range(len(xvalue)):
                xVirgule = xvalue[xi:xi+1]
                if xVirgule == "'":
                    sPosition = xi-2
                    sNomVao = xvalue[0:sPosition].strip()
                    sHaving = xchamps + " LIKE '%" + sNomVao + "%'"
                    break
    
                else:
                    sHaving = xchamps + " = '" + xvalue + "'"
                
        #miverina
        return sHaving
    
    def back_xWhere(self, niveau, reg, dis, com):
        sLike_r=""
        sLike_d=""
        sLike_c=""
        xWhere="" 
        if niveau =="NIVEAU_COM":
            sLike_r = self.SLike_objet(reg, "adm1_en")
            sLike_d = self.SLike_objet(dis, "adm2_en")
            sLike_c = self.SLike_objet(com, "adm3_en")
            xWhere = sLike_r + " AND " + sLike_d + " AND " + sLike_c
        if niveau =="NIVEAU_DIS":
            sLike_r = self.SLike_objet(reg, "adm1_en")
            sLike_d = self.SLike_objet(dis, "adm2_en")
            xWhere = sLike_r + " AND " + sLike_d
        if niveau =="NIVEAU_REG":
            sLike_r = self.SLike_objet(reg, "adm1_en")
            xWhere = sLike_r
        if niveau =="NIVEAU_DIS_COM_UNIQUE":
            sLike_d = self.SLike_objet(dis, "adm2_en")
            sLike_c = self.SLike_objet(com, "adm3_en")
            xWhere = sLike_d + " AND " + sLike_c
        if niveau =="NIVEAU_DIS_UNIQUE":
            sLike_d = self.SLike_objet(dis, "adm2_en")
            xWhere = sLike_d

        return xWhere 
        
    def back_sql_fields(self, niveau):
        slq_fields =""
        if niveau =="NIVEAU_COM":
            slq_fields ="adm1_en, adm2_en, adm3_en, mdg_adm1_pcode, mdg_adm2_pcode, mdg_adm3_pcode"
        if niveau =="NIVEAU_DIS":
            slq_fields ="adm1_en, adm2_en, mdg_adm1_pcode, mdg_adm2_pcode"
        if niveau =="NIVEAU_REG":
            slq_fields ="adm1_en, mdg_adm1_pcode"
        if niveau =="NIVEAU_DIS_COM_UNIQUE":
            slq_fields ="adm1_en, adm2_en, adm3_en, mdg_adm1_pcode, mdg_adm2_pcode, mdg_adm3_pcode"
        if niveau =="NIVEAU_DIS_UNIQUE":
            slq_fields ="adm1_en, adm2_en, mdg_adm1_pcode, mdg_adm2_pcode"
            
        return slq_fields
    
    def back_orderBy(self, niveau):
        orderBy =""
        if niveau =="NIVEAU_COM":
            orderBy ="adm1_en, adm2_en, adm3_en"
        if niveau =="NIVEAU_DIS":
            orderBy ="adm1_en, adm2_en"
        if niveau =="NIVEAU_REG":
            orderBy ="adm1_en"
        if niveau =="NIVEAU_DIS_COM_UNIQUE":
            orderBy ="adm1_en, adm2_en, adm3_en"
        if niveau =="NIVEAU_DIS_UNIQUE":
            orderBy ="adm1_en, adm2_en"
            
        return orderBy
        
    
    def debut_de_processus(self):
        #----verifier si pas vide
        cur = self.con_covid.cursor()
        cur.execute("select count(num_viro_interne) as isany from " + self.tbl_brute)
        #if  cur.fetchone() is not None:
        for row in cur:
           isany = row[0]
           
        if isany > 0:
            self.loop_generer_error_covid()
        
        cur.close()
    
    def loop_generer_error_covid(self):
        print("#---début GENERER LES ERREUR DE DONNEES COVID ---")
        self.back_generer_error_covid("region")
        self.back_generer_error_covid("district")
        self.back_generer_error_covid("commune")
        print("#---fin GENERER LES ERREUR DE DONNEES COVID ---")
        print("Liste des erreurs" + str(self.error_dm))
        
    def back_generer_error_covid(self, champs):
        cur = self.con_covid.cursor()
        #error_sql = "SELECT " + champs + " FROM  " + self.tbl_brute + " GROUP BY " + champs + "  HAVING " + champs +"  is not null and " + champs + " !='' and " + champs + "  !~ '[[:alnum:]]+'"
        error_sql = "SELECT " + champs + " FROM  " + self.tbl_brute + " GROUP BY " + champs + "  HAVING " + champs +"  is not null and " +  champs + "  !~ '[[:alnum:]]+'"
        cur.execute(error_sql)
        see_error = cur.fetchall()
        if see_error:
            for rec_err in see_error:
                if str(rec_err[0]) not in self.error_dm:
                    self.error_dm.append(str(rec_err[0]))
            
        #--close
        cur.close()
   
    def back_single_update(self, xvalue):
        if xvalue is not None:
            for xi in range(len(xvalue)):
                xVirgule = xvalue[xi:xi+1]
                if xVirgule == "'":
                    sPosition = xi-2
                    sNomVao = xvalue[0:sPosition].strip()
                    sHaving =  sNomVao
                    break
    
                else:
                    sHaving = xvalue
                
            return sHaving
    
    def vider_tbl_fsanitaire_corriger(self):
        print("#-----------------début de VIDER LA TABLE fsanitaire------------------------------#")
        cur = self.con_covid.cursor()
        cur.execute("DELETE FROM " + self.tbl_ocha_fsanitaire_corrige)
        self.con_covid.commit()
        cur.close()
        print("#-----------------fin de VIDER LA TABLE  fsanitaire------------------------------#")
    
    def organiser_tbl_fsanitaire_corriger(self):
         self.func_organiser_tbl_fsanitaire_corriger("FULL_MDG_CODE")
         self.func_organiser_tbl_fsanitaire_corriger("EMPTY_MDG_CODE")
    
    def func_organiser_tbl_fsanitaire_corriger(self, aiza):
        print("#-----------------début de ORGANISER LA TABLE fsanitaire corrige " + aiza  + "------------------------------#")
        cur = self.con_covid.cursor()
        if aiza == "FULL_MDG_CODE":
            sql_fsFlds ="ocha_fsanitaire, mdg_fkt_code, date_upload"
            sql_fsWhere ="mdg_fkt_code is not null"
        if aiza == "EMPTY_MDG_CODE":
            sql_fsFlds ="ocha_fsanitaire, date_upload"
            sql_fsWhere ="ocha_fsanitaire is not null and mdg_fkt_code is null"
        
        sql_corrige ="SELECT " + sql_fsFlds  + " FROM " + self.tbl_error_covid_fs + " WHERE " + sql_fsWhere + " ORDER BY ocha_fsanitaire"
        cur.execute(sql_corrige)
        fsCorrige = cur.fetchall()
        for rowFs in fsCorrige:
            if aiza == "FULL_MDG_CODE":
                val_ocha_fsanitaire = str(rowFs[0]).strip()
                val_mdg_fkt_code = str(rowFs[1]).strip()
                val_date_upload = str(rowFs[2]).strip()
            if aiza == "EMPTY_MDG_CODE":
                val_ocha_fsanitaire = str(rowFs[0]).strip()
                val_date_upload = str(rowFs[1]).strip()
            
            strip_val_ocha_fsanitaire = " ".join(val_ocha_fsanitaire.split())
            Like_fs = self.SLike_objet(strip_val_ocha_fsanitaire, "ocha_fsanitaire")
            sql_fsCorrige = "SELECT ocha_fsanitaire FROM " + self.tbl_ocha_fsanitaire_corrige + " WHERE " + Like_fs
            cur.execute(sql_fsCorrige)
            if cur.fetchone() is None:
                if aiza == "FULL_MDG_CODE":
                    cur.execute("INSERT INTO " + self.tbl_ocha_fsanitaire_corrige + " (ocha_fsanitaire, mdg_fkt_code, date_upload) VALUES(%s, %s, %s)", (strip_val_ocha_fsanitaire, val_mdg_fkt_code, val_date_upload))
                    self.con_covid.commit()
                if aiza == "EMPTY_MDG_CODE":
                    cur.execute("INSERT INTO " + self.tbl_ocha_fsanitaire_corrige + " (ocha_fsanitaire, date_upload) VALUES(%s, %s)", (strip_val_ocha_fsanitaire, val_date_upload))
                    self.con_covid.commit()

        cur.close()
        print("#-----------------fin de ORGANISER LA TABLE fsanitaire corrige " + aiza  + "------------------------------#")
              
        
    def func_update_fsanitaire_brute(self, num_viro, fld_fsanitaire, old_fsanitaire, brute_fsanitaire):
        cur = self.con_covid.cursor()
        new_observations =""
        xWhere = " num_viro_interne ='" + num_viro + "'"
        #if deja_fsanitaire is None or deja_fsanitaire =="None" or val_fsanitaire in self.error_dm:
        if brute_fsanitaire is not None and brute_fsanitaire !="None" and brute_fsanitaire not in self.error_dm:
            if brute_fsanitaire != old_fsanitaire:
                param_fs = fld_fsanitaire + " = %s, observations=%s"
                new_observations = "|MAJ " + fld_fsanitaire  + ":" +  old_fsanitaire + ":" + brute_fsanitaire
                Update_fsanitaire ="UPDATE " + self.tbl_covid_ocha  + "  SET " + param_fs +  " WHERE " + xWhere
                cur.execute(Update_fsanitaire, (brute_fsanitaire, new_observations))
                self.con_covid.commit()
                self.nbre_maj_fs += 1
        
    
    def func_update_fsanitaire_brute_grouper(self, val_fsanitaire, val_date_update_ocha):
        cur = self.con_covid.cursor()
        val_num =['032','033','034','039']
        if val_fsanitaire.upper() is not None and val_fsanitaire.upper()!="NONE" and val_fsanitaire.upper() not in self.error_dm:
            sLike_fs = self.SLike_objet(val_fsanitaire.upper(), "fsanitaire")
            sql_fs ="select * FROM " + self.tbl_error_covid_fs + " WHERE " +  sLike_fs
            cur.execute(sql_fs)
            if cur.fetchone() is None:
                #---- jerena raha standard de editer-na  tbl_error_covid_fs sy  tbl_ocha_fsanitaire_corrige
                #---sinon inserer tsotra
                if "GOB" in val_fsanitaire.upper():
                    cur.execute("INSERT INTO " + self.tbl_error_covid_fs + " (fsanitaire, mdg_fkt_code, ocha_fsanitaire, date_upload) VALUES(%s, %s, %s, %s)", (val_fsanitaire.upper(), "MDG11101", "CHU BEFELATANANA", val_date_update_ocha))
                    self.con_covid.commit()
                    self.error_fs +=1
                
                elif "OSIA" in val_fsanitaire.upper():
                    cur.execute("INSERT INTO " + self.tbl_error_covid_fs + " (fsanitaire, mdg_fkt_code, ocha_fsanitaire, date_upload) VALUES(%s, %s, %s, %s)", (val_fsanitaire.upper(), "MDG11103030011", "CHU ANOSIALA", val_date_update_ocha))
                    self.con_covid.commit()
                    self.error_fs +=1

                else:
                    if "DOMICILE" in val_fsanitaire.upper():
                        cur.execute("INSERT INTO " + self.tbl_error_covid_fs + " (fsanitaire, ocha_fsanitaire, date_upload) VALUES(%s, %s, %s)", (val_fsanitaire.upper(), "DOMICILE", val_date_update_ocha))
                        self.con_covid.commit()
                        self.error_fs +=1
                    
                    elif "PRESIDENCE" in val_fsanitaire.upper():
                        cur.execute("INSERT INTO " + self.tbl_error_covid_fs + " (fsanitaire, ocha_fsanitaire, date_upload) VALUES(%s, %s, %s)", (val_fsanitaire.upper(), "PRESIDENCE", val_date_update_ocha))
                        self.con_covid.commit()
                        self.error_fs +=1
 
                    elif any(word in val_fsanitaire.upper() for word in val_num):
                        cur.execute("INSERT INTO " + self.tbl_error_covid_fs + " (fsanitaire, ocha_fsanitaire, date_upload) VALUES(%s, %s, %s)", (val_fsanitaire.upper(), "NUMERO TELEPHONE", val_date_update_ocha))
                        self.con_covid.commit()
                        self.error_fs +=1
                    
                    else:
                        cur.execute("INSERT INTO " + self.tbl_error_covid_fs + " (fsanitaire, date_upload) VALUES(%s, %s)", (val_fsanitaire.upper(), val_date_update_ocha))
                        self.con_covid.commit()
                        self.error_fs +=1
                    
                   
        
    def copy_brute_to_ocha_edit_location (self):
        print("#---Début de COPIER DES NOUVEAUX ENREGISTREMENTS ---")
        nouv_error = 0  
        cur = self.con_covid.cursor()
        cur_stat= self.con_stat.cursor()
        cur.execute("SELECT num_viro_interne, region, district, commune, adresse,  date_update_brute, fsanitaire, fsanitaire_hospital, fsanitaire_declare, date_arrive_prel_ipm FROM " + self.tbl_brute)
        mijery = cur.fetchall()
        for rowSee in mijery:
            val_num_viro_interne = rowSee[0].strip()
            val_region = str(rowSee[1]).strip()
            val_district = str(rowSee[2]).strip()
            val_commune = str(rowSee[3]).strip()
            val_adresse = str(rowSee[4]).strip()
            val_date_update_ocha = str(rowSee[5]).strip()
            val_fsanitaire = str(rowSee[6]).strip()
            val_fsanitaire_hospital = str(rowSee[7]).strip()
            val_fsanitaire_declare = str(rowSee[8]).strip()
            val_date_arrive_prel_ipm = str(rowSee[9]).strip()
            xWhere = " num_viro_interne ='" + val_num_viro_interne + "'"
            #----Copie brute
            cur.execute("SELECT num_viro_interne, region, district, commune, niveau_nettoyage, fsanitaire, observations, fsanitaire_hospital, fsanitaire_declare, date_arrive_prel_ipm FROM " + self.tbl_covid_ocha + " WHERE " + xWhere)
            efa_ao = cur.fetchone()
            if efa_ao:
                deja_region =str(efa_ao[1]).strip()
                deja_district =str(efa_ao[2]).strip()
                deja_commune =str(efa_ao[3]).strip()
                deja_fsanitaire = str(efa_ao[5]).strip()
                deja_observations = str(efa_ao[6]).strip()
                deja_fsanitaire_hospital = str(efa_ao[7]).strip()
                deja_fsanitaire_declare = str(efa_ao[8]).strip()
                deja_date_arrive_prel_ipm = str(efa_ao[9]).strip()
                #----jereo resaka fs
                #if deja_fsanitaire is None or deja_fsanitaire =="None" or val_fsanitaire in self.error_dm:
                self.func_update_fsanitaire_brute(val_num_viro_interne, "fsanitaire", deja_fsanitaire, val_fsanitaire)
                self.func_update_fsanitaire_brute(val_num_viro_interne, "fsanitaire_hospital", deja_fsanitaire_hospital, val_fsanitaire_hospital)
                self.func_update_fsanitaire_brute(val_num_viro_interne, "fsanitaire_declare", deja_fsanitaire_declare, val_fsanitaire_declare)
                
                """
                param_fs = "fsanitaire = %s, date_update_ocha=%s, observations=%s"
                if val_fsanitaire  is not None and val_fsanitaire !="None" and val_fsanitaire not in self.error_dm:
                new_observations = "|MAJ FSanitaire:" +  deja_fsanitaire + ":" + val_fsanitaire
                Update_fsanitaire ="UPDATE " + self.tbl_covid_ocha  + "  SET " + param_fs +  " WHERE " + xWhere
                cur.execute(Update_fsanitaire, (val_fsanitaire, val_date_update_ocha, new_observations))
                self.con_covid.commit()
                self.nbre_maj_fs += 1
                """
                #---rah mbola tsy traite sy vide localisation                
                #if deja_traite =="None" or deja_traite is None:    
                #--champs region
                if deja_region is None or deja_region =="None" or deja_region in self.error_dm:
                    if val_region not in self.error_dm:
                        self.back_update_reg_dis_com("region", val_num_viro_interne, val_region, deja_observations)
                #--champs district
                if deja_district is None or deja_district =="None" or deja_district in self.error_dm:
                    if val_district not in self.error_dm:
                        self.back_update_reg_dis_com("district",val_num_viro_interne, val_district, deja_observations)
                #--champs commune
                if deja_commune is None or deja_commune =="None" or deja_commune in self.error_dm:
                    if val_commune not in self.error_dm:
                        self.back_update_reg_dis_com("commune", val_num_viro_interne, val_commune, deja_observations)
                #--deja_date_arrive_prel_ipm
                if deja_date_arrive_prel_ipm is None or deja_date_arrive_prel_ipm =="None" or deja_date_arrive_prel_ipm in self.error_dm:
                    if val_date_arrive_prel_ipm not in self.error_dm:
                        self.back_update_reg_dis_com("date_arrive_prel_ipm", val_num_viro_interne, val_date_arrive_prel_ipm, deja_observations)
              
            else:
                #----if  cur.fetchone() is None:
                cur.execute("INSERT INTO " + self.tbl_covid_ocha + " (num_viro_interne, region, district, commune, adresse, date_update_ocha, fsanitaire, fsanitaire_hospital, fsanitaire_declare, date_arrive_prel_ipm) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (val_num_viro_interne, val_region, val_district, val_commune, val_adresse, val_date_update_ocha, val_fsanitaire, val_fsanitaire_hospital, val_fsanitaire_declare, val_date_arrive_prel_ipm))
                self.con_covid.commit()
                self.new_enreg +=1
                
           
            #----debut GROUPER ERREUR fsanitaire
            self.func_update_fsanitaire_brute_grouper(val_fsanitaire, val_date_update_ocha)
            self.func_update_fsanitaire_brute_grouper(val_fsanitaire_hospital, val_date_update_ocha)
            self.func_update_fsanitaire_brute_grouper(val_fsanitaire_declare, val_date_update_ocha)
            
            """
            if val_fsanitaire.upper() is not None and val_fsanitaire.upper()!="NONE" and val_fsanitaire.upper() not in self.error_dm:
            sLike_fs = self.SLike_objet(val_fsanitaire.upper(), "fsanitaire")
            sql_fs ="select * from " + self.tbl_error_covid_fs + " where " +  sLike_fs
            cur.execute(sql_fs)
            if cur.fetchone() is None:
            cur.execute("INSERT INTO " + self.tbl_error_covid_fs + " (date_upload, fsanitaire) VALUES(%s, %s)", (val_date_update_ocha, val_fsanitaire.upper()))
            self.con_covid.commit()
            self.error_fs +=1
            """
            
            # Grouper les nouveaux erreurs
            val_date_upload = datetime.now().strftime('%Y%m%d_%H%M%S')
            XREG = val_region.upper()
            XDIS = val_district.upper()
            XCOM = val_commune.upper()
            self.back_group_error_ocha(XREG, "region", nouv_error, "region")
            self.back_group_error_ocha(XDIS, "district", nouv_error, "district")
            self.back_group_error_ocha(XCOM, "commune", nouv_error, "reg_dis_com")
            #---grouper si les 03 champs non pas vides
            if XREG !="NONE" and XDIS !="NONE" and XCOM !="NONE":
                if XREG not in self.error_dm and XDIS not in self.error_dm and XCOM not in self.error_dm:
                    #--pour stat
                    sLike_r_stat = self.SLike_objet(XREG, "adm1_en")
                    sLike_d_stat = self.SLike_objet(XDIS, "adm2_en")
                    sLike_c_stat = self.SLike_objet(XCOM, "adm3_en")
                    xwhereError_stat = sLike_r_stat + " AND " + sLike_d_stat + " AND " + sLike_c_stat
                    #---jerena raha tsy membres stat
                    sql_stat ="SELECT adm1_en, adm2_en, adm3_en FROM " + self.tbl_ref  + " GROUP BY adm1_en, adm2_en, adm3_en HAVING " + xwhereError_stat
                    cur_stat.execute(sql_stat)
                    if cur_stat.fetchone() is None:
                        #---atsofoka
                        sLike_r = self.SLike_objet(XREG, "region")
                        sLike_d = self.SLike_objet(XDIS, "district")
                        sLike_c = self.SLike_objet(XCOM, "commune")
                        xwhereError = sLike_r + " AND " + sLike_d + " AND " + sLike_c
                        sql_ocha_error ="select * from  " +  self.tbl_error_covid_adm + " WHERE "  + xwhereError
                        cur.execute(sql_ocha_error)
                        if cur.fetchone() is None:
                            cur.execute("INSERT INTO " + self.tbl_error_covid_adm + " (region, district, commune, date_upload) VALUES(%s, %s, %s, %s)", (XREG, XDIS, XCOM, val_date_upload))
                            self.con_covid.commit()
                            nouv_error +=1
            
        # fin de traitement
        cur.close()
        cur_stat.close()
        print("nouv enregistrements " + str(self.new_enreg))
        print("nouv erreur de localisation reg, dis, com " + str(nouv_error))
        print("fs mise à jour " + str(self.nbre_maj_fs))
        print("reg mise à jour " + str(self.nbre_maj_reg))
        print("dis mise à jour " + str(self.nbre_maj_dis))
        print("com mise à jour " + str(self.nbre_maj_com))
        print("#---Fin de COPIER DES NOUVEAUX ENREGISTREMENTS  ----")
        
    
    def back_group_error_ocha(self, val_fld, fld, isa_error_ici, nom_tbl):
        val_date_upload = datetime.now().strftime('%Y%m%d_%H%M%S')
        cur = self.con_covid.cursor()
        tbl_update = "tbl_covid_error_ocha_" + nom_tbl
        if val_fld !="NONE":
            if val_fld not in self.error_dm:
                sLike_fld = self.SLike_objet(val_fld, fld)
                xwhereError = sLike_fld 
                sql_ocha_error ="select * from  " +  tbl_update + " WHERE "  + xwhereError
                cur.execute(sql_ocha_error)
                if cur.fetchone() is None:
                    cur.execute("INSERT INTO " + tbl_update + " (" + fld + ", date_upload) VALUES(%s, %s)", (val_fld, val_date_upload))
                    self.con_covid.commit()
                    isa_error_ici +=1
        
        #--fermer
        cur.close()
    
    
    def back_update_reg_dis_com(self, champs, cleWhere, val_update, deja_observations):
        cur = self.con_covid.cursor()
        xWhereCle = " num_viro_interne ='" + cleWhere + "'"
        param_lol =  champs + " = %s, observations=%s"
        new_observations = "|MAJ " + champs  +":" + val_update
        Update_champs ="UPDATE " + self.tbl_covid_ocha  + "  SET " + param_lol +  " WHERE " + xWhereCle
        cur.execute(Update_champs, (val_update, new_observations))
        self.con_covid.commit()
        if champs=="region":
            self.nbre_maj_reg += 1
        if champs=="district":
            self.nbre_maj_dis += 1
        if champs=="commune":
            self.nbre_maj_com += 1
    
        
    def func_boucle_nettoyage_brute_partie0_in_stat(self):
        print("#---début DES TRAITEMENTS BRUTES REG DIS COM pas null ----")
        cur_covid = self.con_covid.cursor()
        cur_stat= self.con_stat.cursor()
        in_error_dm =str(self.error_dm)[1:-1]
        all_stat_flds ="adm1_en, adm2_en, adm3_en, mdg_adm1_pcode, mdg_adm2_pcode, mdg_adm3_pcode"
        all_error_flds ="region, district, commune, mdg_adm1_pcode, mdg_adm2_pcode, mdg_adm3_pcode"
        param_update = "mdg_reg_code = %s, mdg_dis_code=%s, mdg_com_code = %s, niveau_nettoyage =%s"
        where_reg =" region is not null AND region not in (" + in_error_dm + ")"
        where_dis =" district is not null AND district not in (" + in_error_dm + ")"
        where_com =" commune is not null AND commune not in (" + in_error_dm + ")"
        where_all = where_reg + " AND " + where_dis + " AND " + where_com
        xsql ="SELECT upper(region) as region, upper(district) as district, upper(commune) as commune, num_viro_interne FROM " + self.tbl_covid_ocha  + " WHERE " + where_all
        cur_covid.execute(xsql)
        owh = cur_covid.fetchall()
        if owh:
            for fldSee in owh:
                see_reg = str(fldSee[0]).strip()
                see_dis = str(fldSee[1]).strip()
                see_com = str(fldSee[2]).strip()
                strWHERE_Update  = "num_viro_interne = '" +  str(fldSee[3]).strip() + "'"
                
                err_sLike_r = self.SLike_objet(see_reg, "region")
                err_sLike_d = self.SLike_objet(see_dis, "district")
                err_sLike_c = self.SLike_objet(see_com, "commune")
                err_Where = err_sLike_r + " AND " + err_sLike_d + " AND " + err_sLike_c
                
                stat_sLike_r = self.SLike_objet(see_reg, "adm1_en")
                stat_sLike_d = self.SLike_objet(see_dis, "adm2_en")
                stat_sLike_c = self.SLike_objet(see_com, "adm3_en")
                stat_Where = stat_sLike_r + " AND " + stat_sLike_d + " AND " + stat_sLike_c
                #--par stat
                sql_stat= "SELECT " + all_stat_flds + "  FROM " + self.tbl_ref + " GROUP BY " + all_stat_flds + " HAVING " + stat_Where
                cur_stat.execute(sql_stat)
                stat_fld = cur_stat.fetchone()
                if stat_fld:
                    sql_Update ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                    cur_covid.execute(sql_Update, (str(stat_fld[3]), str(stat_fld[4]), str(stat_fld[5]), 'NIV_01_COM'))
                    self.con_covid.commit()
                #--par table error ensemble
                else:
                    sql_err = "SELECT " + all_error_flds + "  FROM " + self.tbl_error_covid_adm + " GROUP BY " + all_error_flds + " HAVING " + err_Where
                    cur_covid.execute(sql_err)
                    err_fld = cur_covid.fetchone()
                    if err_fld:
                        if str(err_fld[3]).upper() != "NONE" and str(err_fld[4]).upper() != "NONE" and str(err_fld[5]).upper() != "NONE":
                            sql_Update ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                            cur_covid.execute(sql_Update, (str(err_fld[3]), str(err_fld[4]), str(err_fld[5]), 'NIV_01_COM_TABLE_ERREUR'))
                            self.con_covid.commit()
                
        #--close recordset
        cur_covid.close()
        cur_stat.close()
        print("#---fin DES TRAITEMENTS BRUTES REG DIS COM pas null ----")
  
    def func_boucle_nettoyage_brute_partie1_in_stat(self, niveau):
        print("#---début DES TRAITEMENTS BRUTES NIVEAU : " + niveau + " ----")
        in_error_dm =str(self.error_dm)[1:-1]
        fld=""
        fld_upper=""
        sqlWhere=""
        if niveau == "region":
            fld = "region"
            fld_upper = "region"
            sqlWhere = "niveau_nettoyage is null and region not in (" + in_error_dm + ")"
            sql_brute = "SELECT num_viro_interne,  upper(region) as region FROM "+ self.tbl_covid_ocha  + " WHERE " + sqlWhere + " ORDER BY num_viro_interne"
        if niveau == "district":
            fld = "district"
            fld_upper = "district"
            sqlWhere = "niveau_nettoyage is null and district not in (" + in_error_dm + ")"
            sql_brute = "SELECT num_viro_interne,  upper(district) as district FROM "+ self.tbl_covid_ocha  + " WHERE " + sqlWhere +  " ORDER BY num_viro_interne"
        if niveau == "commune":
            sqlWhere = "niveau_nettoyage is not null and commune not in (" + in_error_dm + ")"
            sql_brute = "SELECT num_viro_interne,  upper(commune) as commune, mdg_reg_code, mdg_dis_code, niveau_nettoyage FROM "+ self.tbl_covid_ocha + " WHERE "  + sqlWhere +  " ORDER BY num_viro_interne"
            
        #--lancer sql
        cur_covid = self.con_covid.cursor()
        cur_covid.execute(sql_brute)
        records = cur_covid.fetchall()
        col_names = []
        for elt in cur_covid.description:
            col_names.append(elt[0])
        #print(col_names)
        les_lignes = ""
        for enreg in records:
            for isa in range(len(enreg)):
                les_lignes = les_lignes + str(enreg[isa]) + ";"
            
            #print(les_lignes)
            les_lignes =""
            if str(enreg[1]).isalpha():
                if niveau == "region" or niveau== "district":
                    self.back_func_trouve_stat(str(enreg[1]).strip(), str(enreg[0]).strip(), niveau, "VIDE", "VIDE")
                if niveau == "commune":
                    self.back_func_trouve_stat(str(enreg[1]).strip(), str(enreg[0]).strip(), niveau, str(enreg[2]).strip(), str(enreg[3]).strip())
           
                
        #--close recordset
        cur_covid.close()
        print("#---fin DES TRAITEMENTS BRUTES NIVEAU : " + niveau + " -----")
    
    def back_func_trouve_stat(self, val_fld, num_viro, niveau, reg_code, dis_code):
        cur_covid = self.con_covid.cursor()
        cur_stat = self.con_stat.cursor()
        fld_stat = ""
        all_sql_flds =""
        fld_update=""
        sLike_See= ""
        niv_update =""
        sql_Update =""
        strWHERE_Update ="num_viro_interne ='" + num_viro + "'"
        if niveau == "region" or niveau == "district":
            if niveau == "region":
                fld_stat = "adm1_en"
                all_sql_flds ="adm1_en, mdg_adm1_pcode" 
                fld_update="mdg_reg_code"
                niv_update ="NIV_01_REG"
                sLike_See = self.SLike_objet(val_fld, "adm1_en")
                
            if niveau == "district":
                fld_stat = "adm2_en"
                all_sql_flds ="adm2_en, mdg_adm2_pcode"
                fld_update="mdg_dis_code"
                niv_update ="NIV_01_DIS"
                sLike_See = self.SLike_objet(val_fld, "adm2_en")
           
            #--DRESSER SQL
            sql_Update =""
            param_update = fld_update + " = %s, niveau_nettoyage =%s"
            sql_ocha = "SELECT "+ all_sql_flds + " FROM  " + self.tbl_ref + " GROUP BY " +   all_sql_flds  + " HAVING " + sLike_See + " ORDER BY " + fld_stat
            cur_stat.execute(sql_ocha)
            #trouve_val = cur_stat.fetchall()
            trouve_val = cur_stat.fetchone()
            if trouve_val:
                sql_Update ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                cur_covid.execute(sql_Update, (str(trouve_val[1]), niv_update))
                self.con_covid.commit()
        
        if niveau == "commune":
            fld_stat = "adm3_en"
            all_sql_flds ="adm3_en, mdg_adm3_pcode, mdg_adm2_pcode, mdg_adm1_pcode"
            fld_update="mdg_com_code"
            niv_update ="NIV_01_COM"
            sLike_c = self.SLike_objet(val_fld, "adm3_en")
            param_update = "mdg_com_code = %s, niveau_nettoyage =%s"
            if reg_code.upper() !="NONE" and  dis_code.upper() !="NONE":
                sLike_See = sLike_c + " AND mdg_adm1_pcode ='" + reg_code.upper() + "' AND mdg_adm2_pcode ='" + dis_code.upper() + "'"
                #--DRESSER SQL
                sql_ocha = "SELECT "+ all_sql_flds + " FROM  " + self.tbl_ref + " GROUP BY " +   all_sql_flds  + " HAVING " + sLike_See + " ORDER BY " + fld_stat
                cur_stat.execute(sql_ocha)
                trouve_val = cur_stat.fetchone()
                if trouve_val:
                    sql_Update ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                    cur_covid.execute(sql_Update, (str(trouve_val[1]), niv_update))
                    self.con_covid.commit()
                
            if reg_code.upper() =="NONE" and  dis_code.upper() !="NONE":
                sLike_See = sLike_c + " AND mdg_adm2_pcode ='" + dis_code.upper() + "'"
                #--DRESSER SQL
                sql_ocha = "SELECT "+ all_sql_flds + " FROM  " + self.tbl_ref + " GROUP BY " +   all_sql_flds  + " HAVING " + sLike_See + " ORDER BY " + fld_stat
                cur_stat.execute(sql_ocha)
                trouve_val = cur_stat.fetchone()
                if trouve_val:
                    sql_Update ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                    cur_covid.execute(sql_Update, (str(trouve_val[1]), niv_update))
                    self.con_covid.commit()
      
        #---feremer
        cur_stat.close()
        cur_covid.close()
            
          
    """    
    def regenerer_tbl_error_commune(self):
        print("#---début de REGENERER LES ERREURS niv COMMUNE -----")
        cur = self.con_covid.cursor()
        in_error_dm =str(self.error_dm)[1:-1]
        sql_aiza = "SELECT upper(commune) as commune FROM tbl_covid_error_ocha_reg_dis_com WHERE mdg_adm3_pcode is null ORDER BY commune"
        cur.execute(sql_aiza)
        bobak = cur.fetchall()
        if bobak:
            for lol in bobak:
                com_toSee = str(lol[0]).strip()
                #comparer avec ocha
                sLike_c = self.SLike_objet(com_toSee, "adm3_en")
                sqlWhere = "(niveau_nettoyage like '%REG%' OR niveau_nettoyage like '%DIS%') AND  commune is not null AND commune not in (" + in_error_dm + ") AND " + sLike_c 
                sql_ocha = "SELECT upper(commune) as commune, niveau_nettoyage  FROM "  + self.tbl_covid_ocha + " WHERE " + sqlWhere

                
            #liste_ici = les_var_ici[0:-1]
            #if len(liste_ici) > 0:
        
        #--mikatona
        cur.close()
        print("#---fin de REGENERER LES ERREURS niv COMMUNE -----")
    """          
    def compare_reg_code_lost_district_commune(self, niveau):
        print("#---début DE COMPARAISON mdg_code avec reste " +  niveau + "---") 
        cur_covid = self.con_covid.cursor()
        cur_stat= self.con_stat.cursor()
        in_error_dm =str(self.error_dm)[1:-1]
        fld_stat =""
        fld_err =""
        pcode =""
        tbl_error = ""
        niv_nettoyage_stat =""
        niv_nettoyage_err =""
        if niveau =="district":
            all_sql_flds ="adm2_en, mdg_adm2_pcode, mdg_adm1_pcode"
            all_sql_flds_err="district, mdg_adm2_pcode"
            param_update ="mdg_dis_code = %s, niveau_nettoyage = %s"
            where_lost  = "mdg_reg_code is not null and mdg_dis_code is null and district is not null and district not in (" + in_error_dm + ")"
            flds_Select  =" num_viro_interne, upper(district) as district, mdg_reg_code"
            fld_stat ="adm2_en"
            fld_err ="district"
            pcode ="mdg_adm1_pcode"
            niv_nettoyage_stat ="NIV_02_DIS_AVEC_MDG_REG_CODE"
            niv_nettoyage_err ="NIV_02_DIS_AVEC_TABLE_ERREUR_DISTRICT"
            tbl_error = "tbl_covid_error_ocha_district"
        
        if niveau =="commune":
            all_sql_flds ="adm3_en, mdg_adm3_pcode, mdg_adm2_pcode"
            all_sql_flds_err="commune, mdg_adm3_pcode"
            param_update ="mdg_com_code = %s, niveau_nettoyage = %s"
            where_lost = "mdg_dis_code is not null and mdg_com_code is null and commune is not null and commune not in (" + in_error_dm + ")"
            flds_Select =" num_viro_interne, upper(commune) as commune, mdg_dis_code"
            fld_stat ="adm3_en"
            fld_err ="commune"
            pcode ="mdg_adm2_pcode"
            niv_nettoyage_stat ="NIV_02_COM_AVEC_MDG_DIS_CODE"
            niv_nettoyage_err ="NIV_02_COM_AVEC_TABLE_ERREUR_COMMUNE"
            tbl_error = "tbl_covid_error_ocha_reg_dis_com"
        
        #--run sql
        sql_lost ="SELECT " + flds_Select + " FROM " + self.tbl_covid_ocha  + " WHERE " + where_lost
        cur_covid.execute(sql_lost)
        bob_lost = cur_covid.fetchall()
        if bob_lost:
            for fld_lost in bob_lost:
                strWHERE_Update ="num_viro_interne ='" + str(fld_lost[0]) + "'"
                lost_val = str(fld_lost[1]).strip()
                lost_code = str(fld_lost[2]).strip()
                #--look at  stat
                sLike_d = self.SLike_objet(lost_val, fld_stat)
                sqlHaving = pcode + " ='" +  lost_code  + "' AND " + sLike_d
                sqlStat = "SELECT " + all_sql_flds + " FROM  " + self.tbl_ref + " GROUP BY " +   all_sql_flds  + " HAVING " + sqlHaving
                cur_stat.execute(sqlStat)
                stat_fld = cur_stat.fetchone()
                if stat_fld:
                    sql_Update ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                    cur_covid.execute(sql_Update, (str(stat_fld[1]), niv_nettoyage_stat))
                    self.con_covid.commit()
                #--jerena table erreur
                else:
                    err_sLike_d = self.SLike_objet(lost_val, fld_err)
                    sql_err = "SELECT "+  all_sql_flds_err + "  FROM " +  tbl_error  + " WHERE "  +  err_sLike_d
                    cur_covid.execute(sql_err)
                    err_fld = cur_covid.fetchone()
                    if err_fld:
                        if str(err_fld[1]).upper() != "NONE":
                            sql_Update ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                            cur_covid.execute(sql_Update, (str(err_fld[1]), niv_nettoyage_err))
                            self.con_covid.commit()

        #--close
        cur_covid.close()
        cur_stat.close()
        print("#---fin DE COMPARAISON mdg_code avec reste " +  niveau + "---") 
    
              
    def func_boucle_nettoyage_brute_partie2_in_error(self):
        self.back_trouve_ocha_in_error_ocha("region", "region")
        self.back_trouve_ocha_in_error_ocha("district", "district")
        self.back_trouve_ocha_in_error_ocha("commune", "commune")         
            
    def back_trouve_ocha_in_error_ocha(self, fld, upper_fld):
        print("#---début DES TRAITEMENTS par table des erreurs: " +  fld + "---")
        nom_tbl_error =""
        pcode =""
        update_pcode=""
        niveau_nettoyage_ici =""
        if fld =="region":
            nom_tbl_error ="region"
            pcode = "mdg_adm1_pcode"
            update_pcode ="mdg_reg_code"
            niveau_nettoyage_ici ="NIV_01_REG_TABLE_ERREUR"
        if fld =="district":
            nom_tbl_error ="district"
            pcode = "mdg_adm2_pcode"
            update_pcode ="mdg_dis_code"
            niveau_nettoyage_ici ="NIV_01_DIS_TABLE_ERREUR"
        if fld =="commune":
            nom_tbl_error ="reg_dis_com"
            pcode = "mdg_adm3_pcode"
            update_pcode ="mdg_com_code"
            niveau_nettoyage_ici ="NIV_01_COM_TABLE_ERREUR"
            
        cur_ocha = self.con_covid.cursor()
        in_error_dm =str(self.error_dm)[1:-1]
        tbl_error = "tbl_covid_error_ocha_" + nom_tbl_error
        sql_vide_where = " WHERE niveau_nettoyage is null and " + fld + " is not null and " + fld + " not in (" + in_error_dm + ")"
        sql_champs = " num_viro_interne, upper(" + fld + ") as " + upper_fld  + ", niveau_nettoyage"
        sql_brute = "SELECT "  + sql_champs  + " FROM "+ self.tbl_covid_ocha  +  sql_vide_where + " ORDER BY num_viro_interne"
        
        cur_ocha = self.con_covid.cursor()
        cur_ocha.execute(sql_brute)
        omg = cur_ocha.fetchall()
        if omg:
            for omgFld in omg:
                strWHERE_Update ="num_viro_interne = '" + str(omgFld[0])  + "'"
                sLike_error = self.SLike_objet(str(omgFld[1]).strip(), fld)
                where_error = sLike_error + " AND " + pcode  + " is not null"
                xSQL_error = "SELECT " + fld + ", " +  pcode + " FROM " + tbl_error  + " WHERE " + where_error
                cur_ocha.execute(xSQL_error)
                see_error = cur_ocha.fetchone()
                if see_error:
                    param_update_ici = update_pcode + " = %s, niveau_nettoyage =%s"
                    sql_Update_ici ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update_ici + " WHERE " + strWHERE_Update
                    cur_ocha.execute(sql_Update_ici, (str(see_error[1]), niveau_nettoyage_ici))
                    self.con_covid.commit()
        
        #-- fermer
        cur_ocha.close()
        print("#---fin DES TRAITEMENTS par table des erreurs: " +  fld + "---")
    
    def compare_mdg_reg_dis_adresse(self, niveau):
        print("#---déubt de TRAITER ERREUR COVID mdg_code vs ADRESSE, niveau : " + niveau + "  ---")
        cur_ocha = self.con_covid.cursor()
        sql_champs = " num_viro_interne, upper(commune) as commune, upper(adresse) as adresse, mdg_reg_code, mdg_dis_code, mdg_com_code"
        if niveau =="NIVEAU_DIS":
            sTring_sql ="SELECT " + sql_champs + " FROM " + self.tbl_covid_ocha  + " WHERE niveau_nettoyage like '%DIS%'"
        
        if niveau == "NIVEAU_REG":
            sTring_sql ="SELECT " + sql_champs + " FROM " + self.tbl_covid_ocha  + " WHERE niveau_nettoyage like '%REG%'"
        
        cur_ocha.execute(sTring_sql)
        records = cur_ocha.fetchall()
        for enreg in records:
            les_var_ici =""
            liste_ici =""
            valiny_ici = ""
            strWHERE_Update =""
            if niveau =="NIVEAU_DIS":
                strWHERE_Update ="num_viro_interne = '" + str(enreg[0])  + "' AND mdg_reg_code ='" +  str(enreg[3]) + "' AND mdg_dis_code ='" +  str(enreg[4]) + "'"
            if niveau =="NIVEAU_REG":
                strWHERE_Update ="num_viro_interne = '" + str(enreg[0])  + "' AND mdg_reg_code ='" +  str(enreg[3]) + "'"
            spilt_adresse = str(enreg[2]).split()
            
            for xi in range(len(spilt_adresse)):
                if len(str(spilt_adresse[xi])) > 3 and str(spilt_adresse[xi]).isalpha():
                    les_var_ici = les_var_ici + "'" +  str(spilt_adresse[xi]) + "'" + ","
            
            liste_ici = les_var_ici[0:-1]
            if len(liste_ici) > 0:
               
                if niveau =="NIVEAU_DIS":
                    valiny_ici = self.back_tenter_adresse_brute_niv_ocha(liste_ici, niveau, str(enreg[3]), str(enreg[4]))
                    if valiny_ici[:3] =="MDG":
                        param_update = "mdg_com_code = %s,  niveau_nettoyage =%s"
                        sql_Update_tente4 ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                        cur_ocha.execute(sql_Update_tente4, (valiny_ici, 'NIV_02_DIS_TABLE_ERREUR_ADRESSE_VERS_COMMUNE'))
                        self.con_covid.commit()
                    
                if niveau =="NIVEAU_REG":
                    valiny_ici = self.back_tenter_adresse_brute_niv_ocha(liste_ici, niveau, str(enreg[3]), "")
                    if valiny_ici[:3] =="MDG":
                        param_update = "mdg_dis_code = %s,  niveau_nettoyage =%s"
                        sql_Update_tente4 ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                        cur_ocha.execute(sql_Update_tente4, (valiny_ici, 'NIV_02_REG_TABLE_ERREUR_ADRESSE_VERS_DISTRICT'))
                        self.con_covid.commit()
                    
            #--pour prochaine liste
            les_var_ici = ""
            
        cur_ocha.close()
        print("#---fin de TRAITER ERREUR COVID mdg_code vs ADRESSE, niveau : " + niveau + "  ---")
            
    def back_tenter_adresse_brute_niv_ocha(self, liste_ici, niveau, mdg_reg_code, mgd_dis_code):
        cur_stat = self.con_stat.cursor()
        trouve_ici ="TSY_MISY"
        slq_fields =""
        sHaving =""
        slq_fields ="adm1_en, adm2_en, adm3_en, mdg_adm1_pcode, mdg_adm2_pcode, mdg_adm3_pcode"
        #--MITADY COMMUNE
        if niveau =="NIVEAU_DIS":
            sHaving ="mdg_adm1_pcode = '" + mdg_reg_code + "' AND mdg_adm2_pcode = '" + mgd_dis_code + "' AND adm3_en IN ("   + liste_ici  + ")"
        #---MITADY DISTRICT
        if niveau =="NIVEAU_REG":
            sHaving ="mdg_adm1_pcode = '" + mdg_reg_code + "' AND adm2_en IN ("   + liste_ici  + ")"
        #--EXECUTE SQL
        sql_ocha = "SELECT " + slq_fields + " FROM  " + self.tbl_ref + " GROUP BY " +   slq_fields  + " HAVING " + sHaving 
        cur_stat.execute(sql_ocha)
        trouve_val = cur_stat.fetchone()
        if trouve_val:
            if niveau =="NIVEAU_DIS":
                trouve_ici = trouve_val[5]
            if niveau =="NIVEAU_REG":
                trouve_ici = trouve_val[4] 
        
        #print(trouve_ici + "===>" + niveau + "  " + mdg_reg_code +"  " + mgd_dis_code)
        cur_stat.close()
        return trouve_ici
    
    def func_trouve_adresse_brute(self, niv_traitement):
        print("#---déubt de TRAITER ERREUR COVID ADRESSE BRUTE ADRESSE GLOBALE ---")
        cur_ocha = self.con_covid.cursor()
        in_error_dm =str(self.error_dm)[1:-1]
        class_dis_01 = ['LOT', 'LOGEMENT', 'LGMT', 'VILLA', 'CITE', 'BIS', 'TER', 'III', 'II', 'BLOC', 'PARCELLE', 'SECTEUR', 'FOYER', 'RESIDENCE']
        sql_fields ="num_viro_interne,  upper(adresse) as adresse, niveau_nettoyage, mdg_dis_code"
        sTring_sql ="SELECT " + sql_fields + " FROM " + self.tbl_covid_ocha  + " WHERE mdg_dis_code is null AND adresse is not Null and  adresse not in (" + in_error_dm + ")"
        cur_ocha.execute(sTring_sql)
        records = cur_ocha.fetchall()
        for enreg in records:
            strWHERE_Update ="num_viro_interne = '" + str(enreg[0])  + "'"
            spilt_adresse = str(enreg[1]).split()
            les_var_reg =""
            liste_reg =""
            valiny_reg =""
            for xi in range(len(spilt_adresse)):
                if len(str(spilt_adresse[xi])) > 2:
                    les_var_reg = les_var_reg + '"' +  str(spilt_adresse[xi]).replace('"', '') + '"' + ","
              
            liste_reg = les_var_reg[0:-1]
            if len(liste_reg) > 0:
                #--tester si adresse dans la ville ou non
                array_liste_reg = eval('[' + liste_reg + ']')
                ville_01 = set(array_liste_reg).intersection(class_dis_01)
                if len(ville_01) > 0 :
                    #--pour urbaine
                    valiny_reg = self.back_tentation_adresse_brute(array_liste_reg, "01", niv_traitement)
                
                if valiny_reg[:3] =="MDG":
                    param_update = "mdg_reg_code = %s, mdg_dis_code = %s, mdg_com_code = %s,  niveau_nettoyage =%s"
                    sql_Update_tente4 ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                    cur_ocha.execute(sql_Update_tente4, (valiny_reg.split(",")[0].strip(), valiny_reg.split(",")[1].strip(), valiny_reg.split(",")[2].strip(), 'NIV_03_ADRESSE_GLOBALE'))
                    self.con_covid.commit()
                
            les_var_reg = ""
            
        cur_ocha.close()
        print("#---fin de TRAITER ERREUR COVID ADRESSE BRUTE  ADRESSE GLOBALE --")
    
    def back_tentation_adresse_brute(self, liste_be, val_dis_class, niv_traitement):
        trouve_reg ="TSY_MISY"
        cur_stat = self.con_stat.cursor()
        list_Compare = liste_be
        whereTest =" HAVING dis_class ='" + val_dis_class + "'"
        all_loc_dis_ici = []
        all_orientation =['AMBONY', 'AMBANY', 'AVOFOANY', 'AFOVONY', 'AFOVOANY', 'ANTSINANA', 'ANDREFANA', 'AVARATRA', 'ATSIMO', 'ATSINANANA', 'CENTRE', 'SUD', 'NORD', 'OUEST', 'EST', 'CITE', 'BLOC', 'TANANA']
        slq_fields="adm1_en, adm2_en, adm3_en, adm4_en, localite, mdg_adm1_pcode, mdg_adm2_pcode, mdg_adm3_pcode, mdg_adm4_pcode, dis_class"
        xorderBy="adm1_en, adm2_en, adm3_en, adm4_en, localite"
        
        slq_groupe_dis ="SELECT adm2_en, dis_class " + " FROM  " + self.tbl_ref_loc  + " GROUP BY adm2_en, dis_class "  + whereTest + " ORDER BY adm2_en"
        cur_stat.execute(slq_groupe_dis)
        all_dis = cur_stat.fetchall()
        for dis_ici in all_dis:
            all_loc_dis_ici = []
            all_loc_dis_ici.append(str(dis_ici[0]))
            xHaving  ="adm2_en  ='" + str(dis_ici[0]) + "'"
            sql_ocha = "SELECT " + slq_fields + " FROM  " + self.tbl_ref_loc + " GROUP BY " +   slq_fields  +  " HAVING "  + xHaving  + " ORDER BY " + xorderBy
            cur_stat.execute(sql_ocha)
            all_liste = cur_stat.fetchall()
            isany = 0
            for reqAll in all_liste:
                ##--all fkt
                spilt_fkt = str(reqAll[3]).split()
                for fxi in range(len(spilt_fkt)):
                    if len(str(spilt_fkt[fxi])) > 3 and not str(spilt_fkt[fxi]) in all_orientation and not str(spilt_fkt[fxi]) in all_loc_dis_ici:
                        all_loc_dis_ici.append(str(spilt_fkt[fxi])) 
                
                #--all loc
                spilt_loc = str(reqAll[4]).split()
                for xi in range(len(spilt_loc)):
                    if len(str(spilt_loc[xi])) > 3 and not str(spilt_loc[xi]) in all_orientation and not str(spilt_loc[xi]) in all_loc_dis_ici:
                        all_loc_dis_ici.append(str(spilt_loc[xi])) 
                
            
            all_loc_dis_ici = json.dumps(all_loc_dis_ici)
            """
            mifanojo = set(list_Compare).intersection(all_loc_dis_ici)
            if len(mifanojo) > 0:
                trouve_reg = str(reqAll[5])  + "," +  str(reqAll[6])  + "," +  str(reqAll[7])
                isany +=1
                #break
            """
            layhita=""
            for elem in range(len(list_Compare)):
                if str(list_Compare[elem]) in all_loc_dis_ici:
                    isany +=1
                    layhita = layhita +  str(list_Compare[elem]) + ","
            
            if isany > int(niv_traitement):
                trouve_reg = str(reqAll[5])  + "," +  str(reqAll[6])  + "," +  str(reqAll[7])
                break
            #---pour le prochain district
            isany = 0
            all_loc_dis_ici = []
      
        cur_stat.close()
        return trouve_reg
    
    def func_trouve_adresse_brute_reste_district(self):
        print("#---début DES TRAITEMENTS BRUTES RESTE DISTRICT----")
        in_error_dm =str(self.error_dm)[1:-1]
        sql_flds = "num_viro_interne,  upper(region) as region,  upper(district) as district, mdg_dis_code"
        sqlWhere_reg = " region is not null and region not in (" + in_error_dm + ")"
        sqlWhere_dis = " mdg_dis_code is null and district is not null and district not in (" + in_error_dm + ")"
        sql_brute = "SELECT " + sql_flds + "  FROM "+ self.tbl_covid_ocha  + " WHERE " + sqlWhere_reg + " AND " + sqlWhere_dis  + " ORDER BY num_viro_interne"
        cur_covid = self.con_covid.cursor()
        cur_covid.execute(sql_brute)
        records = cur_covid.fetchall()
        for enreg in records:
            reste_num_viro = str(enreg[0]).strip()
            reste_reg = str(enreg[1]).strip()
            reste_dis = str(enreg[2]).strip()
            strWHERE_Update ="num_viro_interne = '" + reste_num_viro + "'"
            err_sLike_r = self.SLike_objet(reste_reg, "region")
            err_sLike_d = self.SLike_objet(reste_dis, "district")
            reste_err_Where = err_sLike_r + " AND " + err_sLike_d
            sql_err = "SELECT region, district, mdg_adm1_pcode, mdg_adm2_pcode  FROM " + self.tbl_error_covid_adm + " WHERE " + reste_err_Where
            cur_covid.execute(sql_err)
            err_fld = cur_covid.fetchone()
            if err_fld:
                see_mdg_reg_code = str(err_fld[2]).upper()
                see_mdg_dis_code = str(err_fld[3]).upper()
                if see_mdg_reg_code != "NONE" and see_mdg_dis_code != "NONE":
                    param_update = "mdg_reg_code =%s, mdg_dis_code =%s, niveau_nettoyage =%s"
                    sql_Update ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                    cur_covid.execute(sql_Update, (see_mdg_reg_code, see_mdg_dis_code, 'NIV_02_DIS_RESTE_TABLE_ERREUR'))
                    self.con_covid.commit()
           
                
        #--close recordset
        cur_covid.close()
        print("#---fin DES TRAITEMENTS BRUTES RESTE DISTRICT-----")
    
    
    
    def regrener_com_code_to_reg_dis_code(self):
        all_comtana =['MDG11101001','MDG11101002','MDG11101003','MDG11101004','MDG11101005','MDG11101006']
        all_distana =['MDG1001A','MDG1002A','MDG1003A','MDG1004A','MDG1005A','MDG1006A']
        print("#---début REGENERER LES CODES OCHA----------------------")
        cur_ocha = self.con_covid.cursor()
        xsql ="SELECT num_viro_interne, mdg_reg_code, mdg_dis_code, mdg_com_code FROM "  +  self.tbl_covid_ocha + " WHERE mdg_com_code is not null and (mdg_dis_code is null or mdg_reg_code is null)"
        cur_ocha.execute(xsql)
        lelik = cur_ocha.fetchall()
        if lelik:
            for fldpor in lelik:
                strWHERE_Update ="num_viro_interne = '" +  str(fldpor[0]).strip() + "'"
                val_com_code = str(fldpor[3]).strip()
                if val_com_code in all_comtana:
                    pos_cua = all_comtana.index(val_com_code)
                    new_reg_code = "MDG11"
                    new_dis_code = str(all_distana[pos_cua])
                else:
                    new_reg_code = str(val_com_code[0:5])
                    new_dis_code = str(val_com_code[0:8])
                #print(str(lol) + "  "  + val_com_code + "  " + new_reg_code + "  " + new_dis_code)
                param_update = "mdg_reg_code = %s, mdg_dis_code = %s"
                sql_Update_tente4 ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                cur_ocha.execute(sql_Update_tente4, (new_reg_code, new_dis_code))
                self.con_covid.commit()
                
                
                
        #--close
        cur_ocha.close()
        print("#---fin REGENERER LES CODES OCHA----------------------")
    
    """
    #-----------------------------------------------------------////////////////////////PARTIE FORMATION SANITAIRE ----------------------------------------------------------
    """
    def traiter_fs_vide_regex(self):
        print("#---début TRAITEMENT REGENERER FORMATION SANITAIRE---------------------")
        cur_covid = self.con_covid.cursor()
        flds_fs= "fsanitaire, mdg_fkt_code, ocha_fsanitaire"
        sql_videFs = "SELECT "+ flds_fs  + " FROM  " + self.tbl_error_covid_fs + "  WHERE ocha_fsanitaire is null ORDER BY fsanitaire"
        cur_covid.execute(sql_videFs)
        fck_all_fs = cur_covid.fetchall()
        if fck_all_fs:
            for flds_fck in fck_all_fs:
                val_sanitaire = str(flds_fck[0]).upper()
                sLike_fs_error = self.SLike_objet(val_sanitaire, "fsanitaire")
                hita = self.back_fuck_fs_regex(val_sanitaire)
                if str(hita[0:8]) != "TSY_MISY":
                    ici_ocha_fsanitaire =  str(hita.split(";")[0]).upper()
                    ici_mdg_code = str(hita.split(";")[1]).upper()
                    if ici_mdg_code !="NONE":
                        try:
                            param_lol =  "ocha_fsanitaire  = %s, mdg_fkt_code = %s"
                            Update_tbl_err_fs ="UPDATE tbl_covid_error_ocha_fsanitaire  SET " + param_lol +  " WHERE " + sLike_fs_error
                            cur_covid.execute(Update_tbl_err_fs, (ici_ocha_fsanitaire, ici_mdg_code))
                            self.con_covid.commit()
                        except:
                            pass
        
        cur_covid.close()        
        print("#---fin TRAITEMENT REGENERER FORMATION SANITAIRE---------------------")

    def back_fuck_fs_regex(self, list_vide):
        back_ocha_fsanitaire =  "TSY_MISY"
        back_mdg_code = "TSY_MISY"
        cur_covid = self.con_covid.cursor()
        flds_fs= "ocha_fsanitaire, mdg_fkt_code, regex_fld"
        sql_fullFs = "SELECT "+ flds_fs  + " FROM " + self.tbl_regex_fs
        cur_covid.execute(sql_fullFs)
        fck_full_fs = cur_covid.fetchall()
        for omg_fs in fck_full_fs:
            array_liste_fs = eval('[' + omg_fs[2] + ']')
            if any(word in list_vide for word in array_liste_fs):
                back_ocha_fsanitaire = str(omg_fs[0])
                back_mdg_code = str(omg_fs[1])
                break
        
        return back_ocha_fsanitaire + ";"  + back_mdg_code
        #--close cnx 
        cur_covid.close()
        
    
    def func_boucle_fs_rendre_mdg_ocha_null(self):
        print("#---début TRAITEMENT FORMATION SANITAIRE rendre initiale---------------------")
        cur_covid = self.con_covid.cursor()
        Update_ocha_mdg_fs ="UPDATE " + self.tbl_covid_ocha  + "  SET ocha_fsanitaire = null, ocha_fsanitaire_declare = null, ocha_fsanitaire_hospital = null, mdg_fkt_code_fsanitaire = null, mdg_fkt_code_fsanitaire_hospital = null, mdg_fkt_code_fsanitaire_declare = null"
        cur_covid.execute(Update_ocha_mdg_fs)
        self.con_covid.commit()
        #--FERMER 
        cur_covid.close() 
        print("#---fin TRAITEMENT FORMATION SANITAIRE rendre initiale---------------------")
        
    def func_boucle_brute_fs(self):
        self.func_boucle_brute_fs_boucle("fsanitaire", "ocha_fsanitaire", "mdg_fkt_code_fsanitaire")
        self.func_boucle_brute_fs_boucle("fsanitaire_hospital", "ocha_fsanitaire_hospital", "mdg_fkt_code_fsanitaire_hospital")
        self.func_boucle_brute_fs_boucle("fsanitaire_declare", "ocha_fsanitaire_declare", "mdg_fkt_code_fsanitaire_declare")
        
    
    def func_boucle_brute_fs_boucle(self, fld_fsanitaire, fld_ocha_fsanitaire, fld_fkt_code_fsanitaire):
        print("#---début TRAITEMENT FORMATION SANITAIRE pour " + fld_fsanitaire  + "---------------------")
        cur_covid = self.con_covid.cursor()
        in_error_dm =str(self.error_dm)[1:-1]
        sql_fdls = "num_viro_interne, upper(" + fld_fsanitaire  + ") as fsanitaire, " + fld_ocha_fsanitaire + ", niveau_nettoyage_fs" 
        sql_where =  fld_fsanitaire + "  is not null and " + fld_fsanitaire  + "  not in  (" + in_error_dm + ")"
        sql_brute = "SELECT " + sql_fdls  + "  FROM " + self.tbl_covid_ocha  + " WHERE " + sql_where
        cur_covid.execute(sql_brute)
        records = cur_covid.fetchall()
        if records:
            col_names = []
            for elt in cur_covid.description:
                col_names.append(elt[0])
            #print(col_names)
            for enreg in records:
                #---test reg, dis, com
                val_num_viro = str(enreg[0]).strip()
                val_sanitaire = str(enreg[1]).upper().strip()
                val_ocha_fsanitaire = str(enreg[2]).upper().strip()
                if val_ocha_fsanitaire is None or val_ocha_fsanitaire == "NONE" or val_ocha_fsanitaire =='':
                    strWHERE_Update = " num_viro_interne ='" + val_num_viro + "'"
                    #var_sanitaire = str(enreg[6])
                    sLike_fs = self.SLike_objet(val_sanitaire, "fs_nom_total")
                    slq_fields ="upper(fs_nom_total) as fs_nom_total, mdg_fkt_code"
                    sql_fs = "SELECT "+ slq_fields + " FROM  " + self.view_ref_fs +   " WHERE " + sLike_fs
                    cur_covid.execute(sql_fs)
                    trouve_fs = cur_covid.fetchone()
                    if trouve_fs:
                        val_fs_nom_total = str(trouve_fs[0])
                        val_mdg_fkt_code = str(trouve_fs[1])
                        param_update = fld_ocha_fsanitaire + " = %s, " + fld_fkt_code_fsanitaire  + " = %s, niveau_nettoyage_fs = %s"
                        sql_Update_fs ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                        cur_covid.execute(sql_Update_fs, (val_fs_nom_total, val_mdg_fkt_code, 'FS_NIV_01'))
                        self.con_covid.commit()
                        self.norme_fs +=1
                    else:
                        #-- ndana karoy any @ tbl_ocha_fsanitaire_corrige
                        sLike_fs_error = self.SLike_objet(val_sanitaire, "ocha_fsanitaire")
                        sql_error_fs = "select ocha_fsanitaire, mdg_fkt_code FROM tbl_ocha_fsanitaire_corrige WHERE " + sLike_fs_error
                        cur_covid.execute(sql_error_fs)
                        see_fs_error = cur_covid.fetchone()
                        if see_fs_error:
                            val_ocha_fsanitaire = str(see_fs_error[0]).upper()
                            val_mdg_fkt_code = str(see_fs_error[1]).upper()
                            self.back_func_boucle_brute_fs_boucle(fld_ocha_fsanitaire, fld_fkt_code_fsanitaire, val_ocha_fsanitaire, val_mdg_fkt_code,  val_num_viro)
                        
                        else:
                            #-- ndana karoy any @ tbl_covid_error_ocha_fsanitaire
                            sLike_fs_error = self.SLike_objet(val_sanitaire, "fsanitaire")
                            sql_error_fs = "select fsanitaire, ocha_fsanitaire, mdg_fkt_code FROM " + self.tbl_error_covid_fs + " WHERE " + sLike_fs_error
                            cur_covid.execute(sql_error_fs)
                            see_fs_error = cur_covid.fetchone()
                            if see_fs_error:
                                val_ocha_fsanitaire = str(see_fs_error[1]).upper()
                                val_mdg_fkt_code = str(see_fs_error[2]).upper()
                                self.back_func_boucle_brute_fs_boucle(fld_ocha_fsanitaire, fld_fkt_code_fsanitaire, val_ocha_fsanitaire, val_mdg_fkt_code, val_num_viro)
           
        else:
            print("tsisy FSanitaire")
            
        #--FERMER 
        cur_covid.close()
        print("#---fin TRAITEMENT FORMATION SANITAIRE pour " + fld_fsanitaire  + "---------------------")
        
    def back_func_boucle_brute_fs_boucle(self, fld_ocha_fsanitaire, fld_fkt_code_fsanitaire, val_ocha_fsanitaire, val_mdg_fkt_code, num_viro):
        cur_covid = self.con_covid.cursor()
        strWHERE_Update = " num_viro_interne ='" + num_viro + "'"
        if val_mdg_fkt_code == "NONE" or val_mdg_fkt_code is None:
            param_update = fld_ocha_fsanitaire + " = %s, niveau_nettoyage_fs =%s"
            sql_Update_fs ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
            cur_covid.execute(sql_Update_fs, (val_ocha_fsanitaire, 'FS_NIV_02_SANS_LOCALISATION'))
            self.con_covid.commit()
            self.error_fs +=1
        else:
            param_update = fld_ocha_fsanitaire + " = %s, " + fld_fkt_code_fsanitaire  + " = %s, niveau_nettoyage_fs =%s"
            sql_Update_fs ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
            cur_covid.execute(sql_Update_fs, (val_ocha_fsanitaire, val_mdg_fkt_code, 'FS_NIV_02_AVEC_LOCALISATION'))
            self.con_covid.commit()
            self.error_fs +=1
    
    def diovy_fsanitaire_mibanaka(self):
        self.func_diovy_fsanitaire_mibanaka("ocha_fsanitaire")
        self.func_diovy_fsanitaire_mibanaka("ocha_fsanitaire_hospital")
        self.func_diovy_fsanitaire_mibanaka("ocha_fsanitaire_declare")
        
    def func_diovy_fsanitaire_mibanaka(self, fld_ocha_fsanitaire):
        print("#----------------début de NETTOYAGE pour le champs " + fld_ocha_fsanitaire  + "-------------------")
        #print(sentence.strip())
        #print(" ".join(sentence.split()))
        cur_covid = self.con_covid.cursor()
        sql_all_fs = "SELECT num_viro_interne, " + fld_ocha_fsanitaire  + " FROM " +  self.tbl_covid_ocha
        cur_covid.execute(sql_all_fs)
        records = cur_covid.fetchall()
        for enreg in records:
            val_num_viro_interne = enreg[0]
            val_fld_ocha_fsanitaire  = str(enreg[1]).strip()
            strWHERE_Update = " num_viro_interne ='" + val_num_viro_interne + "'"
            strip_fld_ocha_fsanitaire = " ".join(val_fld_ocha_fsanitaire.split())
            if val_fld_ocha_fsanitaire != strip_fld_ocha_fsanitaire:
                param_update = fld_ocha_fsanitaire + " = %s, observations =%s"
                sql_Update_fs ="UPDATE "+ self.tbl_covid_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                cur_covid.execute(sql_Update_fs, (strip_fld_ocha_fsanitaire, 'CORRECTION fsanitaire'))
                self.con_covid.commit()
         
        cur_covid.close()
        print("#----------------fin de NETTOYAGE pour le champs " + fld_ocha_fsanitaire  + "-------------------")
         


    """
    #-----------------------------------------------------------//////////////////////// REFAIRE POUR LE CAS POSITIF ----------------------------------------------------------
    """   
    
    """
    #-----------------------------------------------------------//////////////////////// PARTIE RESULTAT ----------------------------------------------------------
    """   
    def compte_result(self):
        #print("#----------------début de COMPTAGE DES TRAITEMENTS-------------------")
        cur_ocha = self.con_covid.cursor()
        in_error_dm =str(self.error_dm)[1:-1]
        #not in (" + in_error_dm + ")"
        sql_reg = "count(num_viro_interne) FILTER (WHERE  region is not null and region  not in  (" + in_error_dm + ") and ocha_reg != region) as nbre_reg_trouve,"  
        sql_dis = "count(num_viro_interne) FILTER (WHERE  district is not null and district  not in  (" + in_error_dm + ") and ocha_dis != district) as nbre_dis_trouve,"  
        sql_com = "count(num_viro_interne) FILTER (WHERE  commune is not null and commune  not in  (" + in_error_dm + ") and ocha_com != commune) as nbre_com_trouve"  
        sql_all = "SELECT " + sql_reg + sql_dis + sql_com + " FROM " +  self.tbl_covid_ocha
        cur_ocha.execute(sql_all)
        ireto = cur_ocha.fetchone()
        if ireto:
            self.nbre_reg_trouve  = str(ireto[0])
            self.nbre_dis_trouve  = str(ireto[1])
            self.nbre_com_trouve  = str(ireto[2])
            var_reg  = str(ireto[0])
            var_dis  = str(ireto[1])
            var_com  = str(ireto[2])
        
        
        #--pour fs 
        sql_null="count(num_viro_interne) FILTER (WHERE niveau_nettoyage_fs is Null) as nbre_fs_null,"
        sql_nbre_fs_trouve = "count(num_viro_interne) FILTER (WHERE niveau_nettoyage_fs ='FS_NIV_01') as nbre_fs_trouve,"
        sql_nbre_fs_localise = "count(num_viro_interne) FILTER (WHERE niveau_nettoyage_fs ='FS_NIV_02_AVEC_LOCALISATION') as nbre_fs_localise,"
        sql_nbre_fs_groupe = "count(num_viro_interne) FILTER (WHERE niveau_nettoyage_fs ='FS_NIV_02_SANS_LOCALISATION') as nbre_fs_groupe"
        sql_all_fs = "SELECT " + sql_null + sql_nbre_fs_trouve + sql_nbre_fs_localise +  sql_nbre_fs_groupe + " FROM " +  self.tbl_covid_ocha
        cur_ocha.execute(sql_all_fs)
        ireto_fs = cur_ocha.fetchone()
        if ireto_fs:
            self.nbre_fs_null = str(ireto_fs[0])
            self.nbre_fs_trouve = str(ireto_fs[1])
            self.nbre_fs_localise = str(ireto_fs[2])
            self.nbre_fs_groupe = str(ireto_fs[3])
            var_fs_null = str(ireto_fs[0])
            var_fs_trouve = str(ireto_fs[1])
            var_fs_localise= str(ireto_fs[2])
            var_fs_groupe= str(ireto_fs[3])
    
        print(var_reg + " " + var_dis + "  " + var_dis  + " " + var_fs_null + " " + var_fs_trouve + "  " + var_fs_localise + "  " + var_fs_groupe)
        cur_ocha.close()
        return var_reg + "," + var_dis + "," + var_com + "," + var_fs_null + "," + var_fs_trouve + "," + var_fs_localise + "," + var_fs_groupe
    
    def input_log_from_view(self):
        print("#---début Inscrire dans le log---------------------")
        if not os.path.exists(path_data_tmp):
            #os.mkdir( path_data_tmp, 0755 )
            os.makedirs(path_data_tmp) 
        
        if os.path.isfile(self.dir_log):
            os.remove(self.dir_log)
        #--boucle infos    
        daty_zao = datetime.today().strftime('%Y-%m-%d') 
        with open(self.dir_log, "a") as fh:
            cur_ocha = self.con_covid.cursor()
            cur_ocha = self.con_covid.cursor()
            sql_fld = "nbre_enreg, nbre_reg_corrige, nbre_dis_corrige, nbre_com_corrige, nbre_fs_corrige, nbre_fs_vide, fs_dis_trouve, fs_dis_vide"  
            sql_result="SELECT " + sql_fld + " FROM public.view_journalier_travail_total WHERE par_jour = '" + daty_zao + "'"
            cur_ocha.execute(sql_result)
            records = cur_ocha.fetchone()
            if records:
                fh.write("Nombre des enregistrements : " + str(records[0]).strip() + "\n")
                fh.write("Nombre des régions trouvés : " + str(records[1]).strip() + "\n")
                fh.write("Nombre des district trouvés : " + str(records[2]).strip() + "\n")
                fh.write("Nombre des communes trouvés : " + str(records[3]).strip() + "\n")
                fh.write("Nombre des fsanitaires trouvés : " + str(records[4]).strip() + "\n")
                fh.write("Nombre des fsanitaires non trouvés : " + str(records[5]).strip() + "\n")
                fh.write("Nombre des districts fsanitaires trouvés : " + str(records[6]).strip()  + "\n")
                fh.write("Nombre des districts fsanitaires non trouvés : " + str(records[7]).strip() + "\n")
            else:
                fh.write("#--------------------Récap pas de mise à jour ---------------------:" + "\n")
        
        #--fermer fichier
        fh.close()
        print("#---fin Inscrire dans le log---------------------")
        
    def input_log(self):
        print("#---début Inscrire dans le log---------------------")
        if not os.path.exists(path_data_tmp):
            #os.mkdir( path_data_tmp, 0755 )
            os.makedirs(path_data_tmp) 
        
        if os.path.isfile(self.dir_log):
            os.remove(self.dir_log)
        #--boucle infos    
        total =0
        total_fs = 0
        with open(self.dir_log, "a") as fh:
            cur_ocha = self.con_covid.cursor()
            sql_result = "SELECT CASE WHEN upper(niveau_nettoyage) is null OR  upper(niveau_nettoyage)= 'NONE' THEN 'MANQUANTE' ELSE upper(niveau_nettoyage) END as niveau_nettoyage, count(num_viro_interne) as isa  FROM public.tbl_covid19_ocha group by niveau_nettoyage order by niveau_nettoyage"  
            cur_ocha.execute(sql_result)
            records = cur_ocha.fetchall()
            if records:
                fh.write("#--------------------Pour Localisation administrative ---------------------:" + "\n")
                for enreg in records:
                    if str(enreg[0]) !='MANQUANTE':
                        total = total + int(str(enreg[1]))
                    fh.write(str(enreg[0]) + ": " +  str(enreg[1]) + "\n")
            
                fh.write("#--------------------Récap Localisation administrative ---------------------:" + "\n")
                fh.write("Nombre des régions trouvés : " +  str(self.nbre_reg_trouve) + "\n")
                fh.write("Nombre des district trouvés : " +  str(self.nbre_dis_trouve) + "\n")
                fh.write("Nombre des communes trouvés : " +  str(self.nbre_com_trouve) + "\n")
                fh.write("Nombre des regions à jour : " +  str(self.nbre_maj_reg) + "\n")
                fh.write("Nombre des districts à jour : " +  str(self.nbre_maj_dis) + "\n")
                fh.write("Nombre des commune à jour : " +  str(self.nbre_maj_com) + "\n")
                fh.write("Localisations nettoyées : " +  str(total) + "\n")
            #---pour fs
            slq_fs="SELECT CASE WHEN upper(niveau_nettoyage_fs) is null OR  upper(niveau_nettoyage_fs)= 'NONE' THEN 'MANQUANTE' ELSE upper(niveau_nettoyage_fs) END as niveau_nettoyage_fs, count(num_viro_interne) as isa  FROM public.tbl_covid19_ocha group by niveau_nettoyage_fs order by niveau_nettoyage_fs"
            cur_ocha.execute(slq_fs)
            records_fs = cur_ocha.fetchall()
            if records_fs:
                fh.write("#--------------------Pour Formation sanitaire ---------------------:" + "\n")
                for enreg_fs in records_fs:
                    if str(enreg_fs[0]) !='MANQUANTE':
                        total_fs = total_fs + int(str(enreg_fs[1]))
                    fh.write(str(enreg_fs[0]) + ": " +  str(enreg_fs[1]) + "\n")
            
                fh.write("#--------------------Récap Formation sanitaire ---------------------:" + "\n")
                fh.write("Nombre des fs trouvées correctements : " +  str(self.nbre_fs_trouve) + "\n")
                fh.write("Nombre des fs localisées (après table erreur): " +  str(self.nbre_fs_localise) + "\n")
                fh.write("Nombre des fs groupés sans localisation  (après table erreur) : " +  str(self.nbre_fs_groupe) + "\n")
                fh.write("Nombre des fs non trouvées et non localisées : " +  str(self.nbre_fs_null) + "\n")
                fh.write("Nombre des fs à jour : " +  str(self.nbre_maj_fs) + "\n")
                fh.write("Formation sanitaires nettoyées : " +  str(total_fs) + "\n")
            #fermer recordset
            cur_ocha.close()
        #--fermer fichier
        fh.close()
        print("#---fin Inscrire dans le log---------------------")
     
   
    def lire_log(self):
        if os.path.isfile(self.dir_log):
            f = open(self.dir_log, "r")
            for x in f:
                print(x) 
            f.close()

           
#if __name__ == '__main__':    
#--load def
nn = Adala()
nn.debut_de_processus()
time.sleep(1)
nn.vider_tbl_fsanitaire_corriger()
time.sleep(1)
nn.organiser_tbl_fsanitaire_corriger()
time.sleep(2)
nn.copy_brute_to_ocha_edit_location()
time.sleep(2)
nn.organiser_tbl_fsanitaire_corriger()
time.sleep(2)
nn.func_boucle_nettoyage_brute_partie0_in_stat()
time.sleep(2)
nn.func_boucle_nettoyage_brute_partie1_in_stat("region")
time.sleep(2)
nn.func_boucle_nettoyage_brute_partie1_in_stat("district")
time.sleep(2)
nn.func_boucle_nettoyage_brute_partie1_in_stat("commune")
time.sleep(2)
nn.compare_reg_code_lost_district_commune("district")
time.sleep(2)
nn.compare_reg_code_lost_district_commune("commune")
time.sleep(2)
nn.func_boucle_nettoyage_brute_partie2_in_error()
time.sleep(2)
nn.compare_mdg_reg_dis_adresse("NIVEAU_DIS")
time.sleep(2)
nn.compare_mdg_reg_dis_adresse("NIVEAU_REG")
time.sleep(2)
nn.func_trouve_adresse_brute(1)
time.sleep(2)
nn.func_trouve_adresse_brute_reste_district()
time.sleep(2)
nn.regrener_com_code_to_reg_dis_code()
time.sleep(2)
nn.traiter_fs_vide_regex()
time.sleep(2)
nn.func_boucle_fs_rendre_mdg_ocha_null()
time.sleep(2)
nn.func_boucle_brute_fs()
time.sleep(2)
nn.diovy_fsanitaire_mibanaka()
time.sleep(1)
#nn.compte_result()
#time.sleep(2)
#nn.input_log()

nn.input_log_from_view()
time.sleep(2)
nn.lire_log()


