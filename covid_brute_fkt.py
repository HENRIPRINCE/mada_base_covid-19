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
#import requests
import json
import psycopg2
#from psycopg2.extras import RealDictCursor
import datetime
from datetime import datetime
from operator import and_
from functools import reduce
from time import sleep
from datetime import timedelta
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
		self.role_carte = "PG_ROLE"
		self.mdp_carte = "PG_MDP"
		self.bd_stat = "bd_reference"
		self.bd_covid = "bd_covid19"
		self.con_covid = psycopg2.connect("dbname='" + self.bd_covid + "' user='" + self.role_carte + "' password='" + self.mdp_carte + "' host='" + self.sig_host + "' port='"+ self.sig_port +"'")
		self.con_stat = psycopg2.connect("dbname='" + self.bd_stat + "' user='" + self.role_carte + "' password='" + self.mdp_carte + "' host='" + self.sig_host + "' port='"+ self.sig_port +"'")
		self.isa_averina = 0
		self.isa_niv_reg =0
		self.isa_niv_dis =0
		self.isa_niv_com =0
		self.isa_niv_reg_2 =0
		self.isa_niv_dis_2 =0
		self.new_enreg = 0
		self.norme_fs = 0
		self.error_fs = 0
		self.tbl_brute ="public.tbl_covid19_brute"
		self.tbl_stat ="data_ocha.mdg_admbnda_adm4_with_cp"
		self.tbl_cvd_ocha ="public.tbl_covid19_ocha"
		self.tbl_cvd_eror ="public.tbl_covid_error_ocha"
		self.tbl_stat_loc="data_ocha.mdg_query_loc"
		self.nom_log = "LOG_COVID_" + datetime.now().strftime('%Y%m%d_%H%M') + ".txt"
		self.dir_log = path_data_tmp +"/" + self.nom_log
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

		self.view_fs_ipm ="public.view_fs_ipm"
		self.tbl_error_ocha_fs ="public.tbl_covid_error_ocha_fsanitaire"
		#self.error_dm = ["---", "--", "-", "None", "Manquant", "Manquante", "NA", '']
		self.error_dm = ["None", "NONE", "Manquant", "Manquante","NA",'']
		self.attention = ['67HA', 'ANDAVAMAMBA', 'ANDOHATAPENAKA','ANDRANOMANALINA',"ANDREFAN'AMBOHIJANAHARY",'ANJANAHARY', 'ANOSIBE', 'ANOSIZATO', 'ANTETEZANA','FIADANANA','MANDRANGOBATO','MANJAKARAY','SOANIERANA','ANTOHOMADINIKA']
		self.attention_niv2 = ['AMPANDRANA', 'ANALAMAHITSY','ANKADINDRAMAMY','AMBOHIDAHY', 'IVATO']

		self.fkt_1_2_3 = ['ANDOHATAPENAKA', 'ANOSIBE ANDREFANA','ANOSIZATO ATSINANANA','ANTETEZANAFOVOANY', 'ANDAVAMAMBA ANJEZIKA', 'ANDAVAMAMBA ANATIHAZO', 'MANDRANGOBATO']

		self.class_dis_01 = ['LOT', 'LOGEMENT', 'LGMT', 'LOGT', 'VILLA', 'BIS', 'TER', 'BLOC', 'PARCELLE', 'SECTEUR', 'FOYER', 'RESIDENCE']
		self.class_secteur = ['III', 'II', 'IIIG-I', 'III-I', 'IIIH-I', 'IIA', 'IIB', 'IIC', 'IID', 'IIN','IIO','IIS','IIIJ','IIIL','IIIN']
		self.all_orientation =['AMBONY', 'AMBANY', 'AVOFOANY', 'AFOVONY', 'AFOVOANY', 'ANTSINANA', 'ANDREFANA', 'AVARATRA', 'ATSIMO', 'ATSINANANA', 'CENTRE', 'SUD', 'NORD', 'OUEST', 'EST', 'CITE', 'BLOC', 'TANANA', 'NORD-EST','NORD-OUEST', 'NE','NO', 'SUD-EST','SUD-OUEST', 'SE','SO']
		self.all_orientation_vrai =['AMBONY', 'AMBANY', 'AFOVOANY', 'AFOVOANY', 'AFOVOANY', 'ATSINANA', 'ANDREFANA', 'AVARATRA', 'ATSIMO', 'ATSINANANA', 'AFOVOANY', 'ATSIMO', 'AVARATRA', 'ANDREFANA', 'ATSINANANA', 'CITE', 'BLOC', 'TANANA', 'AVARATRA ATSINANANA', 'AVARATRA AANDREFANA', 'AVARATRA ATSINANANA', 'AVARATRA ANDREFANA', 'ATSIMO ATSINANANA', 'ATSIMO ANDREFANA', 'ATSIMO ATSINANANA', 'ATSIMO ANDREFANA']

            
        
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
        #cur.execute("select exists(select * from tbl_covid19_brute) as misy_data")
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
        
    def split_adresse(self, adresse):
        spilt_adresse = adresse.split()
        val_retur = "TSY_MISY"
        for xi in range(len(spilt_adresse)):
            val_retur = val_retur + '"' +  str(spilt_adresse[xi]).replace('"', '') + '"' + ","
        
        return val_retur

    def func_trouve_adresse_brute_niv_reg_dis_com(self, niveau, val_observation):
        print("#---déubt de TRAITER ADRESSE BRUTE pour MDG_FKT_CODE - niveau : " + niveau)
        cur_ocha = self.con_covid.cursor()
        in_error_dm =str(self.error_dm)[1:-1]
        sql_fields ="num_viro_interne,  mdg_reg_code, mdg_dis_code, mdg_com_code,  mdg_fkt_code, upper(adresse)" 
        sql_Where = "mdg_dis_code IN ('MDG1001A','MDG1002A','MDG1003A','MDG1004A','MDG1005A','MDG1006A','MDG33314')"
        sTring_sql =""
        len_split_adresse = 2
        if niveau == "3_matchs":
            len_split_adresse = 3
            sql_fkt_vide = "mdg_fkt_code is Null"
            sTring_sql ="SELECT " + sql_fields + " FROM " + self.tbl_cvd_ocha  + " WHERE " + sql_fkt_vide + " AND "  + sql_Where + " AND adresse is not null AND adresse not in (" + in_error_dm + ")"
        if niveau == "2_matchs":
            len_split_adresse = 5
            sql_fkt_vide = "mdg_fkt_code is Null"
            sTring_sql ="SELECT " + sql_fields + " FROM " + self.tbl_cvd_ocha  + " WHERE " + sql_fkt_vide + " AND " + sql_Where + " AND adresse is not null AND adresse not in (" + in_error_dm + ")"
        if niveau == "1_matchs":
            len_split_adresse = 5
            sql_fkt_vide = "mdg_fkt_code is Null"
            sTring_sql ="SELECT " + sql_fields + " FROM " + self.tbl_cvd_ocha  + " WHERE " + sql_fkt_vide + " AND " + sql_Where + " AND adresse is not null AND adresse not in (" + in_error_dm + ")"
        if niveau == "1_matchs_attention_class_secteur" or niveau == "1_matchs_attention_fkt_1_2_3" or niveau == "1_matchs_attention_niv2":
            len_split_adresse = 3
            sql_fkt_vide = "mdg_fkt_code is Null"
            sTring_sql ="SELECT " + sql_fields + " FROM " + self.tbl_cvd_ocha  + " WHERE " + sql_fkt_vide + " AND " + sql_Where + " AND adresse is not null AND adresse not in (" + in_error_dm + ")"
        
        cur_ocha.execute(sTring_sql)
        records = cur_ocha.fetchall()
        for enreg in records:
            num_viro_ici = str(enreg[0])
            mdg_reg_ici = enreg[1]
            mdg_dis_ici = enreg[2]
            mdg_com_ici = enreg[3]
            adresse_ici = str(enreg[5])
            if mdg_reg_ici is not None and mdg_dis_ici is not None  and mdg_com_ici is not None:
                strWHERE_Update ="num_viro_interne = '" + num_viro_ici  + "' AND mdg_reg_code ='" +  mdg_reg_ici + "' AND mdg_dis_code ='" + mdg_dis_ici  + "' AND mdg_com_code ='" +  mdg_com_ici + "'"
                if len(adresse_ici) > 3:
                    if niveau == "1_matchs_attention_niv2":
                        liste_reg =""
                        take_it =""
                        take_all =""
                        var_vrai_orientation =""
                        spilt_adresse = adresse_ici.split()
                        for xi in range(len(spilt_adresse)):
                            if str(spilt_adresse[xi]) in self.attention_niv2:
                                take_it = str(spilt_adresse[xi])
                                break
                        
                        if take_it !="":
                            if take_it =="ANKADINDRAMAMY":
                                take_all ="ANKERANA ANKADINDRAMAMY"
                            elif take_it == "ANALAMAHITSY":
                                lesOrientes =""
                                for xi in range(len(spilt_adresse)):
                                    if str(spilt_adresse[xi]) != take_it and len(str(spilt_adresse[xi])) >= 4 and not str(spilt_adresse[xi]) in self.class_dis_01:
                                        lesOrientes += str(spilt_adresse[xi]) + " "
                                    
                                lesOrientes = lesOrientes[0:-1]
                                if lesOrientes !="":
                                    take_all = take_it + " " + lesOrientes
                                else:
                                    take_all = take_it
                                            
                            else:
                                #orientation
                                lesOrientes =""
                                for xi in range(len(spilt_adresse)):
                                    if spilt_adresse[xi] in self.all_orientation:
                                        Index67 = self.all_orientation.index(str(spilt_adresse[xi]))
                                        var_vrai_orientation = str( self.all_orientation_vrai[Index67])
                                        #les_var_reg = les_var_reg + '"' +  str(spilt_adresse[xi]).replace('"', '') + '"' + ","
                                        lesOrientes += str(var_vrai_orientation) + " "
                                        break
                                
                                #lesOrientes = lesOrientes[0:-1]
                                lesOrientes = lesOrientes.rstrip()
                                if lesOrientes !="":
                                    take_all = take_it + " " + lesOrientes
                                else:
                                    take_all = take_it
                                
                            #--send final
                            liste_reg = take_all.rstrip()
                            #print(adresse_ici + "==> "  + liste_reg)
                            valiny_fkt = self.back_tentation_adresse_brute_Niveau1(niveau, num_viro_ici, mdg_reg_ici, mdg_dis_ici, mdg_com_ici, liste_reg)
                            if valiny_fkt !="TSY_MISY":
                                #if valiny_fkt[:3] =="MDG":
                                #print(adresse_ici + "==> "  +  liste_reg + " ===> " + valiny_fkt)
                                param_update = "mdg_fkt_code = %s, niveau_nettoyage =%s"
                                sql_Update_tente4 ="UPDATE "+ self.tbl_cvd_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                                cur_ocha.execute(sql_Update_tente4, (valiny_fkt, val_observation))
                                self.con_covid.commit()
                                
                            
                        
                    if niveau == "1_matchs_attention_fkt_1_2_3":
                        liste_reg =""
                        les_var_reg =""
                        take_it =""
                        take_all =""
                        spilt_adresse = adresse_ici.split()
                        for xi in range(len(spilt_adresse)):
                            if str(spilt_adresse[xi]) in self.fkt_1_2_3:
                                take_it = str(spilt_adresse[xi])
                                break
                        if take_it !="":
                            for xi in range(len(spilt_adresse)):
                                if str(spilt_adresse[xi]) == "I":
                                    take_all = take_it + " I"
                                    break
                            for xi in range(len(spilt_adresse)):
                                if str(spilt_adresse[xi]) == "II":
                                    take_all = take_it + " II"
                                    break
                            for xi in range(len(spilt_adresse)):
                                if str(spilt_adresse[xi]) == "III":
                                    if take_it =="ANDOHATAPENAKA":
                                        take_all = take_it + " III"
                                    else:
                                        take_all = take_it + " II"
                                    break
                            
                            if take_all !="":
                                les_var_reg = take_all
                            else:
                                les_var_reg = take_it + " I"
                            
                            #--send to final
                            liste_reg = les_var_reg.rstrip()
                            #print(adresse_ici + "==> "  + liste_reg)
                            valiny_fkt = self.back_tentation_adresse_brute_Niveau1(niveau, num_viro_ici, mdg_reg_ici, mdg_dis_ici, mdg_com_ici, liste_reg)
                            if valiny_fkt !="TSY_MISY":
                                #if valiny_fkt[:3] =="MDG":
                                #print(adresse_ici + "==> "  +  liste_reg + " ===> " + valiny_fkt)
                                param_update = "mdg_fkt_code = %s, niveau_nettoyage =%s"
                                sql_Update_tente4 ="UPDATE "+ self.tbl_cvd_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                                cur_ocha.execute(sql_Update_tente4, (valiny_fkt, val_observation))
                                self.con_covid.commit()
                        
                    
                    if niveau == "1_matchs_attention_class_secteur":
                        liste_reg =""
                        les_var_reg =""
                        spilt_adresse = adresse_ici.split()
                        #C--pour 67ha
                        vodinovy =""
                        if  ("67" in adresse_ici and "HA" in adresse_ici) or "67HA" in adresse_ici:
                            for xi in range(len(spilt_adresse)):
                                if "(" in str(spilt_adresse[xi]) or ")" in str(spilt_adresse[xi]):
                                    vodinovy = str(spilt_adresse[xi])[1:-1]
                                    #les_var_reg += "----------------------------------------- hita : " + str(spilt_adresse[xi]) + "===============" + vodinovy
                                    if vodinovy in self.all_orientation:
                                        Index67 = self.all_orientation.index(vodinovy)
                                        var_vrai_orientation = str( self.all_orientation_vrai[Index67])
                                        les_var_reg += str(var_vrai_orientation) + " "
                                    
                                if str(spilt_adresse[xi]) in self.all_orientation:
                                    Index67 = self.all_orientation.index(str(spilt_adresse[xi]))
                                    var_vrai_orientation = str( self.all_orientation_vrai[Index67])
                                    les_var_reg += str(var_vrai_orientation) + " "
                            
                            liste_reg = les_var_reg.rstrip()
                            les_var_reg = "67HA " + les_var_reg
                           
                        elif  "ANJANAHARY" in adresse_ici:
                            jerena = ['A','N','O','S']
                            if "II" in adresse_ici:
                                val_aloha = "II"
                                to_add = self.toadd_niv1(val_aloha, spilt_adresse, jerena )
                                les_var_reg = "ANJANAHARY" + " " + to_add
                            
                            if "IIA" in adresse_ici:
                                les_var_reg = "ANJANAHARY" + " IIA"
                            if "IIN" in adresse_ici:
                                les_var_reg = "ANJANAHARY" + " IIN"
                            if "IIO" in adresse_ici:
                                les_var_reg = "ANJANAHARY" + " IIO"
                            if "IIS" in adresse_ici:
                                les_var_reg = "ANJANAHARY" + " IIS"
                              
                        elif "MANJAKARAY" in adresse_ici:
                            jerena = ['B','C','D']
                            if "II" in adresse_ici:
                                val_aloha = "II"
                                to_add = self.toadd_niv1(val_aloha, spilt_adresse, jerena )
                                les_var_reg = "MANJAKARAY" + " " + to_add
                            
                            if "IIB" in adresse_ici:
                                les_var_reg = "MANJAKARAY" + " IIB"
                            if "IIC" in adresse_ici:
                                les_var_reg = "MANJAKARAY" + " IIC"
                            if "IID" in adresse_ici:
                                les_var_reg = "MANJAKARAY" + " IID"
                        
                        elif "FIADANANA" in adresse_ici:
                            if "IIIL" in adresse_ici:
                                les_var_reg = "FIADANANA" + " IIIL"
                            if "IIIN"  in adresse_ici:
                                les_var_reg = "FIADANANA" + " IIIN"
                        elif "SOANIERANA" in adresse_ici:
                            if "III-I"  in adresse_ici:
                                les_var_reg = "SOANIERANA" + " III-I"
                            if "IIIJ" in adresse_ici:
                                les_var_reg = "SOANIERANA" + " IIIJ"     
                        
                        elif "ANDREFAN'AMBOHIJANAHARY" in adresse_ici:
                            if "IIIG-I" in adresse_ici:
                                les_var_reg = "ANDREFAN'AMBOHIJANAHARY" + " IIIG-I"
                            if "IIIH-I"in adresse_ici:
                                les_var_reg = "ANDREFAN'AMBOHIJANAHARY" + " IIIH-I"
                        
                        elif "AMBOHIDAHY" in adresse_ici and mdg_dis_ici == 'MDG1001A':
                            les_var_reg = "AMPARIBE AMBOHIDAHY MAHAMASINA"
                            
                        elif "TSARALALANA" in adresse_ici and mdg_dis_ici == 'MDG1001A':
                            les_var_reg = "FIATA"
                        
                        elif "AMBILANIBE" in adresse_ici and (mdg_dis_ici == 'MDG1004A' or  mdg_dis_ici == 'MDG1001A'):
                            les_var_reg = "ANDAVAMAMBA AMBILANIBE"
                            mdg_dis_ici == 'MDG1004A'
                        
                        elif "AMBODINISOTRY" in adresse_ici and mdg_dis_ici == 'MDG1001A':
                            les_var_reg = "CITE AMBODIN'ISOTRY"
                         
                        elif "ANTOHOMADINIKA" in adresse_ici:
                            if "AFOVOANY" in adresse_ici:
                                les_var_reg = "ANTOHOMADINIKA AFOVOANY"
                            elif "ANTSALOVANA" in adresse_ici:
                                les_var_reg = "ANTOHOMADINIKA ANTSALOVANA FAA"
                            elif "HANGAR" in adresse_ici or "IIIG" in adresse_ici or "III" in adresse_ici:
                                les_var_reg = "ANTOHOMADINIKA IIIG HANGAR"
                            elif "ATSIMO" in adresse_ici:
                                les_var_reg = "ANTOHOMADINIKA ATSIMO"    
                            elif "AVARATRA" in adresse_ici:
                                les_var_reg = "ANTOHOMADINIKA AVARATRA ANTANI"    
                            else:
                                for xi in range(len(spilt_adresse)):
                                    if str(spilt_adresse[xi]) in self.all_orientation:
                                        Index67 = self.all_orientation.index(str(spilt_adresse[xi]))
                                        var_vrai_orientation = str( self.all_orientation_vrai[Index67])
                                        les_var_reg += str(var_vrai_orientation) + " "
                                
                                liste_reg = les_var_reg.rstrip()
                                les_var_reg = "ANTOHOMADINIKA " + les_var_reg
                        
                        else:
                            pass
                        
                        #--send final
                        if str(les_var_reg) !="":
                            liste_reg = les_var_reg
                            liste_reg = liste_reg.rstrip()
                            #print(adresse_ici + "==> " + liste_reg)
                            valiny_fkt = self.back_tentation_adresse_brute_Niveau1(niveau, num_viro_ici, mdg_reg_ici, mdg_dis_ici, mdg_com_ici, liste_reg)
                            if valiny_fkt !="TSY_MISY":
                                #if valiny_fkt[:3] =="MDG":
                                #print(adresse_ici + "==> "  +  liste_reg + " ===> " + valiny_fkt)
                                param_update = "mdg_fkt_code = %s, niveau_nettoyage =%s"
                                sql_Update_tente4 ="UPDATE "+ self.tbl_cvd_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                                cur_ocha.execute(sql_Update_tente4, (valiny_fkt, val_observation))
                                self.con_covid.commit()
                         
                    if niveau == "2_matchs" or niveau == "3_matchs":
                        if niveau == "3_matchs":
                            les_var_reg =""
                            spilt_adresse = adresse_ici.split()
                            for xi in range(len(spilt_adresse)):
                                if len(str(spilt_adresse[xi])) >= len_split_adresse and not str(spilt_adresse[xi]) in self.class_dis_01 and not str(spilt_adresse[xi]) in self.attention:
                                    #if not str(spilt_adresse[xi]) in self.class_dis_01 and not filter(lambda x: str(spilt_adresse[xi]) in x, self.attention):
                                    les_var_reg = les_var_reg + '"' +  str(spilt_adresse[xi]).replace('"', '') + '"' + ","
                    
                        if niveau == "2_matchs":
                            les_var_reg =""
                            spilt_adresse = adresse_ici.split()
                            for xi in range(len(spilt_adresse)):
                                if len(str(spilt_adresse[xi])) >= len_split_adresse and not str(spilt_adresse[xi]) in self.class_dis_01 and not str(spilt_adresse[xi]) in self.attention and not str(spilt_adresse[xi]) in self.attention_niv2 and not str(spilt_adresse[xi]) in self.fkt_1_2_3:
                                    les_var_reg = les_var_reg + '"' +  str(spilt_adresse[xi]).replace('"', '') + '"' + ","
    
                        liste_reg = les_var_reg[0:-1]
                        if len(liste_reg) > 0:
                            array_liste_reg = eval('[' + liste_reg + ']')
                            valiny_fkt = self.back_tentation_adresse_brute(niveau, num_viro_ici, mdg_reg_ici, mdg_dis_ici, mdg_com_ici, array_liste_reg)
                            if valiny_fkt !="TSY_MISY":
                                #if valiny_fkt[:3] =="MDG":
                                #print(adresse_ici + " ===>" + str(array_liste_reg) + " ===> " + valiny_fkt)
                                param_update = "mdg_fkt_code = %s, niveau_nettoyage =%s"
                                sql_Update_tente4 ="UPDATE "+ self.tbl_cvd_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                                cur_ocha.execute(sql_Update_tente4, (valiny_fkt, val_observation))
                                self.con_covid.commit()
                                
                    
                    if niveau == "1_matchs":
                        tavela = []
                        les_var_reg =""
                        spilt_adresse = adresse_ici.split()
                        for xi in range(len(spilt_adresse)):
                            if len(str(spilt_adresse[xi])) >= len_split_adresse and not str(spilt_adresse[xi]) in self.class_dis_01 and not str(spilt_adresse[xi]) in self.class_secteur:
                                if str(spilt_adresse[xi]) not in tavela:
                                    tavela.append(str(spilt_adresse[xi]))
                                les_var_reg = les_var_reg + '"' +  str(spilt_adresse[xi]).replace('"', '') + '"' + ","
                        
                        if len(tavela) > 1:
                            #print(adresse_ici + " ===>" + mdg_com_ici + " == " + str(tavela))
                            if "AMBOHIDAHY" in tavela and "ANKADINDRAMAMY" in tavela:
                                tavela.remove("ANKADINDRAMAMY")
                                fkt_ici = str(''.join(tavela))
                                #print("FOOOZA ================="+  adresse_ici + " ===>" + mdg_com_ici + " == " + fkt_ici)
                                valiny_fkt = self.back_tentation_adresse_brute_Niveau1(niveau, num_viro_ici, mdg_reg_ici, mdg_dis_ici, mdg_com_ici, fkt_ici)
                                if valiny_fkt !="TSY_MISY":
                                    #print("FOOOZA AMBOHIDAHY================="+ adresse_ici + " ===>" + mdg_reg_ici +  " | " + mdg_dis_ici +  " | " + mdg_com_ici + " == " + fkt_ici + " ===> " + valiny_fkt)
                                    param_update = "mdg_fkt_code = %s, niveau_nettoyage =%s"
                                    sql_Update_tente4 ="UPDATE "+ self.tbl_cvd_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                                    cur_ocha.execute(sql_Update_tente4, (valiny_fkt, val_observation))
                                    self.con_covid.commit()
                                
                            else:
                                #print(adresse_ici + " ===>" + mdg_com_ici + " == " + str(tavela))
                                valiny_fkt = self.back_tentation_adresse_brute(niveau, num_viro_ici, mdg_reg_ici, mdg_dis_ici, mdg_com_ici, tavela)
                                if valiny_fkt !="TSY_MISY":
                                    #print(adresse_ici + " ===>" + mdg_com_ici + " == " + str(tavela) + " ===> " + valiny_fkt)
                                    param_update = "mdg_fkt_code = %s, niveau_nettoyage =%s"
                                    sql_Update_tente4 ="UPDATE "+ self.tbl_cvd_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                                    cur_ocha.execute(sql_Update_tente4, (valiny_fkt, val_observation))
                                    self.con_covid.commit()
                        
                        if len(tavela) == 1:
                            fkt_ici = str(''.join(tavela))
                            #print("FOOOZA ================="+  adresse_ici + " ===>" + mdg_com_ici + " == " + fkt_ici)
                            valiny_fkt = self.back_tentation_adresse_brute_Niveau1(niveau, num_viro_ici, mdg_reg_ici, mdg_dis_ici, mdg_com_ici, fkt_ici)
                            if valiny_fkt !="TSY_MISY":
                                #print("FOOOZA HAFA ================="+  adresse_ici + " ===>" + mdg_com_ici + " == " + fkt_ici +  " ===> " +  valiny_fkt)
                                param_update = "mdg_fkt_code = %s, niveau_nettoyage =%s"
                                sql_Update_tente4 ="UPDATE "+ self.tbl_cvd_ocha  + "  SET " + param_update + " WHERE " + strWHERE_Update
                                cur_ocha.execute(sql_Update_tente4, (valiny_fkt, val_observation))
                                self.con_covid.commit()
                        
                            
                       
                            
    
    
                        
                                
                            
                      
                    
        cur_ocha.close()
        print("#---fin de TRAITER ADRESSE BRUTE pour MDG_FKT_CODE - niveau : " + niveau)
    
    def toadd_niv1(self, val_aloha,  list_Compare_ici, jerena):
        val_return = "TSY_MISY"
        for i in range(len(jerena)):
            if jerena[i] in list_Compare_ici:
                to_add = val_aloha + str(jerena[i])
                val_return = to_add
                break
        return val_return
    
    def back_tentation_adresse_brute_Niveau1(self, niveau, num_viro_ici, mdg_reg_ici, mdg_dis_ici, mdg_com_ici, val_fkt):
        trouve_reg ="TSY_MISY"
        cur_stat = self.con_stat.cursor()
        whereFkt = ""
        split_fkt = ""
        slq_fields="adm4_en, localite, mdg_adm4_pcode,  mdg_adm1_pcode, mdg_adm2_pcode, mdg_adm3_pcode, dis_class"
        xWhere_parent = "mdg_adm1_pcode ='" +  mdg_reg_ici + "' AND mdg_adm2_pcode ='" +  mdg_dis_ici + "' AND mdg_adm3_pcode ='" + mdg_com_ici  + "'"
        if "'" in val_fkt:
            split_fkt = val_fkt.split("'")
            #debut = split_fkt[0]
            fin = split_fkt[1]
            whereFkt = "adm4_en LIKE '%" + fin  + "%'"
        else:
            whereFkt = "adm4_en = '" + val_fkt  + "'"
        
        xWhere_ici  = xWhere_parent + " AND " + whereFkt
        xorderBy="mdg_adm1_pcode, mdg_adm2_pcode, mdg_adm3_pcode, mdg_adm4_pcode"
        slq_groupe_dis ="SELECT " + slq_fields + " FROM  " + self.tbl_stat_loc  + " WHERE " + xWhere_ici + " ORDER BY " + xorderBy
        cur_stat.execute(slq_groupe_dis)
        all_infos = cur_stat.fetchone()
        if all_infos:
            val_mdg_fkt_code = str(all_infos[2])
            val_nom_fkt = str(all_infos[0]) 
            #trouve_reg = "fkt: " +  val_nom_fkt   + " | " + val_mdg_fkt_code
            trouve_reg = val_mdg_fkt_code
        
        #--fermer
        cur_stat.close()
        return trouve_reg
        
    def back_tentation_adresse_brute(self, niveau, num_viro_ici, mdg_reg_ici, mdg_dis_ici, mdg_com_ici, adresse_ici):
        trouve_reg ="TSY_MISY"
        cur_stat = self.con_stat.cursor()
        list_Compare = adresse_ici
        all_loc_dis_ici = []
        niv_traitement = 2
        if niveau == "3_matchs": 
            niv_traitement = 2
        if niveau == "2_matchs": 
            niv_traitement = 1
        if niveau == "1_matchs": 
            niv_traitement = 1
        
        slq_fields="adm4_en, localite, mdg_adm4_pcode,  mdg_adm1_pcode, mdg_adm2_pcode, mdg_adm3_pcode, dis_class"
        xWhere_ici = "mdg_adm1_pcode ='" +  mdg_reg_ici + "' AND mdg_adm2_pcode ='" +  mdg_dis_ici + "' AND mdg_adm3_pcode ='" + mdg_com_ici  + "'"
        xorderBy="mdg_adm1_pcode, mdg_adm2_pcode, mdg_adm3_pcode, mdg_adm4_pcode"
        slq_groupe_dis ="SELECT " + slq_fields + " FROM  " + self.tbl_stat_loc  + " WHERE " + xWhere_ici + " ORDER BY " + xorderBy
        cur_stat.execute(slq_groupe_dis)
        all_infos = cur_stat.fetchall()
        isany = 0
        for reqAll in all_infos:
            fkt_ici = str(reqAll[0])
            spilt_fkt = fkt_ici.split()
            for fxi in range(len(spilt_fkt)):
                if not str(spilt_fkt[fxi]) in all_loc_dis_ici and not str(spilt_fkt[fxi]) in self.attention:
                    all_loc_dis_ici.append(str(spilt_fkt[fxi])) 
      
            """
            #--all loc
            spilt_loc = str(reqAll[1]).split()
            for xi in range(len(spilt_loc)):
                if len(str(spilt_loc[xi])) > 3 and not str(spilt_loc[xi]) in all_orientation and not str(spilt_loc[xi]) in all_loc_dis_ici:
                    all_loc_dis_ici.append(str(spilt_loc[xi])) 
            """        
            #---to json
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
                    #if filter(lambda x: str(list_Compare[elem]) in x, all_loc_dis_ici):
                    isany +=1
                    layhita = layhita +  str(list_Compare[elem]) + ","
            
            if isany >= niv_traitement:
                val_mdg_fkt_code = str(reqAll[2])
                val_nom_fkt = str(reqAll[0]) 
                #trouve_reg = "fkt: " +  val_nom_fkt   + " | " + val_mdg_fkt_code
                trouve_reg = val_mdg_fkt_code
            
            #---pour le prochain district
            isany = 0
            #trouve_reg = str(all_loc_dis_ici)
            all_loc_dis_ici = []
        
        #--fermer
        cur_stat.close()
        return trouve_reg
    
    

        
#if __name__ == '__main__':    
#--load def
nn = Adala()
nn.debut_de_processus()
time.sleep(1)
nn.func_trouve_adresse_brute_niv_reg_dis_com("3_matchs", 'NIV_04_ADRESSE_RECUP_FKT_3Matchs')
time.sleep(5)
nn.func_trouve_adresse_brute_niv_reg_dis_com("2_matchs", 'NIV_04_ADRESSE_RECUP_FKT_2Matchs')
time.sleep(5)
nn.func_trouve_adresse_brute_niv_reg_dis_com("1_matchs_attention_class_secteur", 'NIV_04_ADRESSE_RECUP_FKT_1Matchs_1')
time.sleep(5)
nn.func_trouve_adresse_brute_niv_reg_dis_com("1_matchs_attention_niv2", 'NIV_04_ADRESSE_RECUP_FKT_1Matchs_2')
time.sleep(5)
nn.func_trouve_adresse_brute_niv_reg_dis_com("1_matchs_attention_fkt_1_2_3", 'NIV_04_ADRESSE_RECUP_FKT_1Matchs_3')
time.sleep(5)
nn.func_trouve_adresse_brute_niv_reg_dis_com("1_matchs", 'NIV_04_ADRESSE_RECUP_FKT_1Matchs_Final')
#----------------recall back
time.sleep(5)
nn.func_trouve_adresse_brute_niv_reg_dis_com("1_matchs_attention_class_secteur", 'NIV_04_ADRESSE_RECUP_FKT_1Matchs_1_recall')
print("#---------------------------------------------------------------------FIN")





