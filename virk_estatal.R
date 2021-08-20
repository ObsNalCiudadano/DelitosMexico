rm(list = ls())
###########################################
# Proyecto: Limpieza bases Secretariado  ##
# Autora: Dani y Erik                    ##
# Fecha: 16 de abril del 2021            ##
###########################################

#     Script final limpieza Estatal: tasas, absolutos, víctimas          #####
#


# Paquetes:
library(tidyverse)
library(lubridate)
library(haven)
library(googledrive)
library(readxl)
library(rvest)  
library(xml2)
#library(tabulizer)
library(labelled)
library(janitor)

##########################3

fun_tasa <- function(abs, pob) {
  round((abs/pob)*100000, digits = 2)
}

## Directorios ##

usuarix <- "C:/Users/emcdo/"
bg <- paste0(usuarix, "Google Drive/big_carpet/Bases Generales")
output <- paste0(usuarix, "Google Drive/big_carpet/Bases Generales/tidy")

tempo <- tempfile(fileext = ".zip")
drive_deauth()
dl <- drive_download(as_id("1GPRGmMr1c1YyQD249mW3CzWaKEHBDFly"),
                     path = tempo,
                     overwrite = TRUE)
tempo_out <-  unzip(tempo, exdir = tempdir())


###################### Objetos cambian ####
aux_m <- data.frame(
  num_mes = c(-1:12),  
  
  nom_mes = c("nov","dic","ene", "feb", "mar", "abr", "may", "jun",
              "jul", "ago", "sep", "oct", "nov", "dic")
  )

if(day(today()) >= 20){
  num <- month(today()) - 1
} else {
  num <- month(today()) -2
} 

b_mes <- aux_m %>% filter(num_mes == num)

if(yday(today()) >= 51) {
  ao <- year(today())
} else {
  ao <- year(today()) - 1
}
mes_abs <- paste0(b_mes$nom_mes[1], ao)
mes_tasa <-paste0("t_", b_mes$nom_mes[1], ao)






### Bases ###

b_dta_raw <- read_dta('https://www.dropbox.com/s/vd4y3ztd1btlc41/mastertotal.dta?dl=1',
                      col_select = c(state_code:mes_abs, 
                                     t_ene2015:mes_tasa)) %>% 
  mutate(state_code = as.numeric(as.character(state_code)))


edo_code <- read_csv(paste(bg, "edo_code.csv", sep = "/"))
Encoding(edo_code$edo_nom_may) <- "Latin 1"

Encoding(edo_code$edo_nom) <- "Latin 1"
mes_code <- data.frame(nom_mes = c("enero", "febrero", "marzo", "abril", "mayo", "junio",
                                   "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"),
                       num_mes = c(1:12))

## Bases secuestro ################

# Carga de base histórica

b_sec_histo <- read_csv(paste(bg, "secuestro_federal_historico.csv", sep = "/")) %>% 
  mutate(state_code = ifelse(Entidad == 'Chiapas', 7,
                             ifelse(Entidad == 'Coahuila', 5,
                                    ifelse(Entidad == 'Chihuahua', 8,
                                           ifelse(Entidad == 'Colima', 6, state_code)))))



#### Función para obtener el link del nombre de la base actual #####

url <- "https://www.gob.mx/sesnsp/acciones-y-programas/incidencia-delictiva-del-fuero-federal?idiom=es"

if (yday(Sys.Date()) < 51) {
  ff_ao <- year(Sys.Date()) - 1
} else {
  ff_ao <- year(Sys.Date())
}


webpage <- read_html(url)

textos <- webpage %>%
  rvest::html_nodes("a") %>% html_text()

link_df <- data.frame(log = str_detect(textos, as.character(ff_ao)),
                      ind = 1:length(textos)) %>% 
  filter(log == TRUE)

links <- webpage %>%
  rvest::html_nodes("a") %>% html_attr('href')

link_fin <- str_sub(links[link_df[3, 'ind']], 33, -18) ### esto es el link que cambia


### Usando link descargar secuestro FF del mes actual

temp <- tempfile(fileext = ".zip")
dl <- drive_download(
  as_id(link_fin), path = temp, overwrite = TRUE)
out <- unzip(temp, exdir = tempdir())
b_sec_act_c <- read_xlsx(out, skip = 2, sheet = 1) %>% 
  clean_names() %>% filter(delitos %in% edo_code$edo_nom_may)
  
b_sec_act_v <- read_xlsx(out, skip = 2, sheet = 2) %>% 
  clean_names() %>% filter(victimas %in% edo_code$edo_nom_may)

b_sec_carpetas <- b_sec_act_c %>% tibble() %>% 
  select(-total) %>% gather("nom_mes", "carpetas", -delitos) %>% 
  left_join(mes_code, by = "nom_mes") %>% 
  left_join(edo_code, by = c("delitos" = "edo_nom_may")) %>% 
  mutate(ao = year(today()),
         fecha = ceiling_date(ymd(paste(ao, num_mes, 01, sep = "/")), 
                              unit = "1 month") - days(1)) %>% 
  select(state_code = edo_code, Entidad = edo_nom, fecha, carpetas, ao)

b_sec_victimas <- b_sec_act_v %>% select(-total) %>% gather("nom_mes", "vic", -victimas) %>% 
  left_join(mes_code, by = "nom_mes") %>% 
  left_join(edo_code, by = c("victimas" = "edo_nom_may")) %>% 
  mutate(ao = year(today()),
         fecha = ceiling_date(ymd(paste(ao, num_mes, 01, sep = "/")), 
                              unit = "1 month") - days(1)) %>% 
  select(state_code = edo_code, Entidad = edo_nom, fecha, victimas = vic, ao)
  
  
b_sec_actual <- b_sec_carpetas %>% 
  left_join(b_sec_victimas, by = c("state_code", "Entidad", "fecha", "ao")) %>% 
  select(state_code:carpetas, victimas, ao)


#sec_out <- paste0('C:/Users/',usuario, '/Google Drive/big_carpet/Bases Generales/')

##definir output y cargar base absolutos de leo para tener datos para secuestro



########## Base de población para Tasas
pob <- read_csv('https://www.dropbox.com/s/rbg059jn9eikbim/statepop.csv?dl=1') %>% 
  mutate(Entidad = ifelse(StateName == 'Distrito Federal', 'Ciudad de México',
                          ifelse(StateName == 'Coahuila', 'Coahuila de Zaragoza',
                                 ifelse(StateName == 'Michoacán', 'Michoacán de Ocampo',
                                        ifelse(StateName == 'Veracruz', 'Veracruz de Ignacio de la Llave', StateName)))),
         ao = Year) %>% select(ao, Total, Entidad) 

num_nac <- data.frame(Entidad = "Nacional", num_edo = 0)
pob_num <- pob %>% distinct(Entidad) %>% 
  mutate(num_edo = 1:32) %>% 
  bind_rows(num_nac)

########calcular poblacion nacional----

pob_nac <- pob %>% 
  group_by(ao) %>% 
  summarise(Total = sum(Total)) %>% 
  mutate(Entidad = 'Nacional') %>% select(ao, Total, Entidad) 

pob_w_nac <- rbind(pob, pob_nac) #unir pob nacional con estatal

pob_completa <- pob_w_nac %>% left_join(pob_num, by = "Entidad") %>% 
  select(state_code=num_edo, ao, Total)





########preparar bases de leo para sumar a datos escrapeados
labs_leo <- val_labels(b_dta_raw$state_code) %>% bind_rows() %>% 
  gather(key = 'Entidad', value = 'state_code')

sec_estatal <- b_dta_raw %>% filter(crime == 2000) %>% 
  mutate(state_code = as.numeric(as.character(state_code)))#filtrar secuestro de base leo



ent_no <- c('OTRO PAIS', 'NO ESPECIFICADO')

no_in <- negate(`%in%`)

#### Operación para agregar actua y vieja

b_sec_ff <- b_sec_histo %>% bind_rows(b_sec_actual) %>% 
  select(state_code, Entidad, ao, fecha, ci_ff_abs = carpetas, vic_ff_abs = victimas) %>% 
  filter(!is.na(ci_ff_abs))






################################################################################################

#####

#### Secuestro fuero común

edo_code_join <- edo_code %>% distinct(edo_code, edo_nom)

b_sec_fc_ci <- b_dta_raw %>% filter(crimeid == 20001) %>% 
  select(state_code, ene2015:mes_abs) %>% 
  gather("periodo", "ci_fc_abs", -state_code) %>% 
  left_join(edo_code_join, by = c("state_code" = "edo_code")) %>% 
  mutate(mes_nomi = str_sub(periodo, start = 0, end = 3),
         ao = str_sub(periodo, start= 4, end = 9)) %>% 
  left_join(aux_m[3:14,], by = c("mes_nomi" = "nom_mes")) %>% 
  mutate(fecha = ceiling_date(ymd(paste(ao, num_mes, 01, sep = "/")), 
                              unit = "1 month") - days(1)) %>% 
  select(state_code, Entidad = edo_nom, ao, fecha, ci_fc_abs)

b_sec_fc_vic <- b_dta_raw %>% filter(crimeid == 20002) %>% 
  select(state_code, ene2015:mes_abs) %>% 
  gather("periodo", "vic_fc_abs", -state_code) %>% 
  left_join(edo_code_join, by = c("state_code" = "edo_code")) %>% 
  mutate(mes_nomi = str_sub(periodo, start = 0, end = 3),
         ao = str_sub(periodo, start= 4, end = 9)) %>% 
  left_join(aux_m[3:14,], by = c("mes_nomi" = "nom_mes")) %>% 
  mutate(fecha = ceiling_date(ymd(paste(ao, num_mes, 01, sep = "/")), 
                              unit = "1 month") - days(1)) %>% 
  select(state_code, Entidad = edo_nom, ao, fecha, vic_fc_abs)


b_sec_fc <- b_sec_fc_ci %>% left_join(b_sec_fc_vic, by = c("state_code", "Entidad", "ao", "fecha"))  %>% 
  mutate(ao = as.numeric(ao))


b_sec_abs <- b_sec_ff %>% left_join(b_sec_fc, by = c("state_code", "Entidad", "ao", "fecha"))


b_sec <- b_sec_abs %>% left_join(pob_completa, by = c("ao","state_code")) %>%
  mutate(ci_ff_tasa = fun_tasa(ci_ff_abs, Total),
         ci_fc_tasa = fun_tasa(ci_fc_abs, Total),
         vic_ff_tasa = fun_tasa(vic_ff_abs, Total),
         vic_fc_tasa = fun_tasa(vic_fc_abs, Total),
         researchFolders = ci_fc_abs + ci_ff_abs,
         researchFoldersRate = fun_tasa(researchFolders, Total),
         victimCase = vic_ff_abs + vic_fc_abs,
         victimRate = fun_tasa(victimCase, Total)
         )


write_csv(b_sec, paste0(bg, '/secuestro_tidy.csv'))

#######################################################################################################
###############  acá termina secuestro      ###################################
########################################################################################

#######################################################################################################
######## Histórico todos los delitos


b_histo_abs_r <- read_dta('https://www.dropbox.com/s/sjpy45hp4dst2hs/state_totalCIEISP.dta?dl=1',
                        col_select = c(state_code:dic2014))
b_histo_abs <-  b_histo_abs_r %>% 
  gather("periodo", "absolutos", -c(state_code, categoryid, typeid, subtypeid)) %>% 
  mutate(nom_mes = str_sub(periodo, start = 1, end = 3),
         ao = str_sub(periodo, start = 4, end = 8)) %>% 
  left_join(aux_m[3:14,], by = "nom_mes") %>% 
   mutate(fecha = ceiling_date(ymd(paste(ao, num_mes, 01, sep = "/")), 
                                     unit = "1 month") - days(1)) %>% 
  select(state_code:subtypeid, fecha, absolutos) 

b_histo_tasa_r <- read_dta('https://www.dropbox.com/s/almzctcobgh82hr/state_rateCIEISP.dta?dl=1',
                          col_select=c(state_code:tdic2014)) 

b_histo_tasa <- b_histo_tasa_r %>% 
  gather("periodo", "tasa", -c(state_code, categoryid, typeid, subtypeid)) %>% 
  mutate(nom_mes = str_sub(periodo, start = 2, end = 4),
         ao = str_sub(periodo, start = 5, end = 9)) %>%
  left_join(aux_m[3:14,], by = "nom_mes") %>% 
  mutate(fecha = ceiling_date(ymd(paste(ao, num_mes, 01, sep = "/")), 
                              unit = "1 month") - days(1)) %>% 
  select(state_code:subtypeid, fecha, tasa)


b_histo <- b_histo_tasa %>% left_join(b_histo_abs, by = c("state_code", "categoryid", "typeid", "subtypeid", "fecha"))




######## ACÁ VAMOS CHIQUIII :)
#######################################################################################################



#sec1 <- read_csv(paste0(secinput, '/', 'abs_secuestro.csv')) %>%
#  gather(2:61, key = 'fecha', value = 'Valor') %>%
#  spread(X1, Valor)


#####################Lista de labels estadarizados para entidad

states <- tribble( 
  
  ~state_code, ~Entidad,
  
  0,                      'NACIONAL',
  1,                "AGUASCALIENTES",
  2,               "BAJA CALIFORNIA",
  3,           "BAJA CALIFORNIA SUR",
  4,                      "CAMPECHE",  
  5,                      "COAHUILA",
  6,                        "COLIMA",
  7,                       "CHIAPAS",
  8,                     "CHIHUAHUA",
  9,               "CIUDAD DE MEXICO",
  10,                       "DURANGO",
  11,                    "GUANAJUATO",
  12,                      "GUERRERO",
  13,                       "HIDALGO",
  14,                       "JALISCO",
  15,                        "MEXICO",
  16,                     "MICHOACAN",
  17,                        "MORELOS",
  18,                         "NAYARIT",
  19,                      "NUEVO LEON",
  20,                          "OAXACA",
  21,                          "PUEBLA",
  22,                       "QUERETARO",
  23,                    "QUINTANA ROO",
  24,                 "SAN LUIS POTOSI",
  25,                         "SINALOA",
  26,                          "SONORA",
  27,                         "TABASCO",
  28,                      "TAMAULIPAS",
  29,                        "TLAXCALA",
  30,                        "VERACRUZ",
  31,                         "YUCATAN",
  32,                       "ZACATECAS"
  
)

###########################unir secuestros con entidades


print('break 3')
Sys.sleep(1)

############################Claves de delito para la base historica

categoryidlab <- tribble(
  
  ~categoryid, ~categoryid1,
  
  0,             'INCIDENCIA DELICTIVA',
  1,            'DELITOS PATRIMONIALES',
  2,     'DELITOS SEXUALES (VIOLACION)',
  3,                       'HOMICIDIOS',
  4,                         'LESIONES',
  5,                    'OTROS DELITOS',
  6, 'PRIV. DE LA LIBERTAD (SECUESTRO)',
  7,                       'ROBO COMUN',
  8,        'ROBO DE GANADO (ABIGEATO)',
  9,               'ROBO EN CARRETERAS',
  10,  'ROBO EN INSTITUCIONES BANCARIAS',
  11,                       'ROBO TOTAL'
  
)


typelab <- tribble(
  
  ~typeid, ~typeid1,
  
  1,                     'ABIGEATO',
  2,           'ABUSO DE CONFIANZA',
  3,                    'AMENAZAS',
  4,               'CON VIOLENCIA',
  5,                     'CULPOSAS',
  6,                     'CULPOSOS',
  7,      'DAÑO EN PROPIEDAD AJENA',
  8,                      'DESPOJO',
  9,                      'DOLOSAS',
  10,                      'DOLOSOS',
  11,                      'ESTUPRO',
  12,                    'EXTORSION',
  13,                       'FRAUDE',
  14,               'OTROS SEXUALES',
  15, 'RESTO DE LOS DELITOS (OTROS)',
  16,                    'SECUESTRO',
  17,                'SIN VIOLENCIA',
  18,                    'VIOLACION',
  19,                        'TOTAL'
  
)


subtypelab <- tribble(
  
  ~subtypeid, ~subtypeid1,
  
  1,                      'A AUTOBUSES',
  2,                         'A BANCOS',
  3,              'A CAMIONES DE CARGA',
  4,                  'A CASA DE BOLSA',
  5,                 'A CASA DE CAMBIO',
  6,                'A CASA HABITACION',
  7, 'A EMPRESA DE TRASLADO DE VALORES',
  8,                        'A NEGOCIO',
  9,                    'A TRANSEUNTES',
  10,                 'A TRANSPORTISTAS',
  11,         'A VEHICULOS PARTICULARES',
  12,                         'ABIGEATO',
  13,               'ABUSO DE CONFIANZA',
  14,                        'AMENAZAS',
  15,                 'CON ARMA BLANCA',
  16,               'CON ARMA DE FUEGO',
  17,                   'CON VIOLENCIA',
  18,          'DAÑO EN PROPIEDAD AJENA',
  19,                     'DE VEHICULOS',
  20,                          'ESTUPRO',
  21,                        'EXTORSION',
  22,                           'FRAUDE',
  23,                            'OTROS',
  24,                  'OTROS SEXUALES',
  25,     'RESTO DE LOS DELITOS (OTROS)',
  26,                        'SECUESTRO',
  27,                        'SIN DATOS',
  28,                    'SIN VIOLENCIA',
  29,                        'VIOLACION',
  30,                            'TOTAL'
  
)



###########################Limpiar la base historica cambiando las claves por los nombres

old_labs <- b_histo %>%
  inner_join(categoryidlab, by = 'categoryid') %>%
  inner_join(typelab, by = 'typeid') %>%
  inner_join(subtypelab, by = 'subtypeid') %>%
  inner_join(states, by = 'state_code') %>%
  mutate(categoryid = categoryid1,
         typeid = typeid1,
         subtypeid = subtypeid1) %>%
  dplyr::select(-categoryid1, -typeid1, -subtypeid1)






old <- rbind(
  
  old_labs %>%
    filter(categoryid == 'INCIDENCIA DELICTIVA',
           typeid == 'TOTAL',
           subtypeid == 'TOTAL') %>%
    mutate(crimeid = 'Total de delitos'),
  
  old_labs %>%
    filter(categoryid == 'DELITOS SEXUALES (VIOLACION)',
           typeid == 'VIOLACION',
           subtypeid == 'VIOLACION')%>%
    mutate(crimeid = 'Violacion'),
  
  old_labs %>%
    filter(categoryid == 'HOMICIDIOS',
           typeid == 'DOLOSOS',
           subtypeid == 'CON ARMA DE FUEGO')%>%
    mutate(crimeid = 'Homicidio doloso con arma de fuego'),
  
  old_labs %>%
    filter(categoryid == 'LESIONES',
           typeid == 'DOLOSAS',
           subtypeid == 'CON ARMA DE FUEGO')%>%
    mutate(crimeid = 'Lesiones con arma fuego'),
  
  old_labs %>%
    filter(categoryid == 'PRIV. DE LA LIBERTAD (SECUESTRO)',
           typeid == 'SECUESTRO',
           subtypeid == 'SECUESTRO')%>%
    mutate(crimeid = 'Secuestro'),
  
  old_labs %>%
    filter(categoryid == 'ROBO COMUN',
           typeid == 'TOTAL',
           subtypeid == 'A NEGOCIO')%>%
    mutate(crimeid = 'Robo a negocio'),
  
  old_labs %>%
    filter(categoryid == 'ROBO COMUN',
           typeid == 'TOTAL',
           subtypeid == 'DE VEHICULOS')%>%
    mutate(crimeid = 'Robo de Vehículos'),
  
  old_labs %>%
    filter(categoryid == 'DELITOS PATRIMONIALES',
           typeid == 'EXTORSION',
           subtypeid == 'EXTORSION')%>%
    mutate(crimeid = 'Extorsión'),
  
  old_labs %>%
    filter(categoryid == 'HOMICIDIOS',
           typeid == 'DOLOSOS',
           subtypeid == 'TOTAL')%>%
    mutate(crimeid = 'Homicidio doloso'),
  
  old_labs %>%
    filter(categoryid == 'HOMICIDIOS',
           typeid == 'CULPOSOS',
           subtypeid == 'TOTAL')%>%
    mutate(crimeid = 'Homicidio culposo'),
  
  old_labs %>%
    filter(categoryid == 'LESIONES',
           typeid == 'TOTAL',
           subtypeid == 'TOTAL')%>%
    mutate(crimeid = 'Lesiones dolosas total'),
  
  old_labs %>%
    filter(categoryid == 'ROBO COMUN',
           typeid == 'TOTAL',
           subtypeid == 'A CASA HABITACION')%>%
    mutate(crimeid = 'Robo a casa habitación'),
  
  old_labs %>%
    filter(categoryid == 'ROBO COMUN',
           typeid == 'TOTAL',
           subtypeid == 'A TRANSEUNTES')%>%
    mutate(crimeid = 'Robo a transeúnte'),
  
  
  old_labs %>%
    filter(categoryid == 'ROBO TOTAL',
           typeid == 'CON VIOLENCIA',
           subtypeid == 'TOTAL')%>%
    mutate(crimeid = 'Robo total con violencia')
  
) %>%
  dplyr::select(-categoryid, -typeid, -subtypeid)




###########################Labels del delito para la base nueva
labtib <- tribble(
  ~crime, ~crime1, ~orden,
  0,                            'Total de delitos',  1,
  1100,                           'Homicidio doloso',2, 
  1101,         'Homicidio doloso con arma de fuego',3, 
  1110,                   'Homicidio doloso sin Fem',4, 
  1111, 'Homicidio doloso con arma de fuego sin Fem',5, 
  1120,                                'Feminicidio',6, 
  1121,              'Feminicidio con arma de fuego',7, 
  1200,                          'Homicidio culposo',8, 
  2000,                                  'Secuestro',9, 
  3000,                                  'Extorsión',10,
  4001,                         'Robo con violencia',11, 
  4100,                           'Robo de vehículo',12,
  4200,                     'Robo a casa habitación',13,
  4300,                             'Robo a negocio',14,
  4400,                    'Robo a transeúnte total',15,
  4410,              'Robo en espacio o vía pública',16,
  4420,                         'Robo en transporte',17,
  4940, 'Robo en transporte público', 18,
  5000,                                  'Violación',19,
  6000,                         'Violencia familiar',20,
  7000,                          'Trata de personas',21,
  8000,                               'Narcomenudeo',22,
  9000,                           'Lesiones dolosas',23,
  9001,         'Lesiones dolosas con arma de fuego', 24)


################################Labels para la base de datos históricos
alt_list <- tribble(
  
  ~crimeid, ~crime1,
  
  'Total de delitos'                  , 'Total de delitos',
  'Extorsión'                         , 'Extorsión', 
  'Violacion'                         , "Violación",
  'Homicidio culposo'                 , "Homicidio culposo",
  'Homicidio doloso con arma de fuego', "Homicidio doloso con arma de fuego",
  'Homicidio doloso'                  , "Homicidio doloso",
  'Lesiones con arma fuego'           , "Lesiones dolosas con arma de fuego",
  'Lesiones dolosas total'            , "Lesiones dolosas",
  'Secuestro'                         , "Secuestro",
  'Robo a casa habitación'            , "Robo a casa habitación",
  'Robo a negocio'                    , "Robo a negocio",
  'Robo a transeúnte'                 , "Robo en espacio o vía pública",
  'Robo de Vehículos'                 , "Robo de vehículo",
  'Robo total con violencia'          , "Robo con violencia"
  
)

########################Agregar los label estandarizados a las bases mediante un Join

data2 <- inner_join(estatalnew, labtib, by = 'crime')
old2 <- inner_join(old, alt_list, by = 'crimeid')

########################Agregar Robo a transeunte como robo a transeunte total a la base de datos historicos
alt_old <- old %>%
  filter(crimeid == 'Robo a transeúnte') %>%
  mutate(crime1 = 'Robo a transeúnte total')

old_w_robo <- rbind(alt_old, old2) %>% dplyr::select(-crimeid)


########################Gather base actual
b_est_abs <- b_dta_raw %>% 
  select(state_code:mes_abs) %>% 
  gather(ene2015:mes_abs, key = 'periodo', value = 'Abs') 

b_est_abs_ci <- b_est_abs %>% 
  filter(unit == 1) %>% select(-unit) %>% 
  rename(researchFolders = Abs)

b_est_abs_vic <- b_est_abs %>% 
  filter(unit == 2) %>% select(-c('unit', 'crimeid')) %>% 
  rename(victimCase = Abs)


b_est_tas <- b_dta_raw %>% 
  select(state_code:unit, t_ene2015:mes_tasa) %>% 
  gather(t_ene2015:mes_tasa, key = 'periodo', value = 'Tasa')

b_est_tas_ci <- b_est_tas %>% 
  filter(unit == 1) %>% select(-unit) %>% 
  mutate(periodo = str_replace(periodo, 't_', '')) %>% 
  rename(researchFoldersRate = Tasa)

b_est_tas_vic <- b_est_tas %>% 
  filter(unit == 2) %>% select(-c('unit', 'crimeid')) %>% 
  mutate(periodo = str_replace(periodo, 't_', '')) %>% 
  rename(victimRate = Tasa)


virk_est_new <- b_est_abs_ci %>% 
  inner_join(b_est_tas_ci, by = c('state_code', 'crimeid', 'crime', 'periodo')) %>% 
  left_join(b_est_abs_vic, by = c('state_code', 'crime', 'periodo')) %>% 
  left_join(b_est_tas_vic, by = c('state_code', 'crime', 'periodo')) %>% 
  mutate(nom_mes = str_sub(periodo, start = 1, end = 3),
         ao = str_sub(periodo, start = -4, end = -1)) %>% 
  left_join(aux_m[aux_m$num_mes > 0,], by = 'nom_mes') %>% 
  left_join(labtib, by = 'crime') %>% 
  left_join(edo_code_join, by = c('state_code' = 'edo_code')) %>% 
  mutate(fecha = as.Date(paste(ao, num_mes, 1, sep = '-')),
         fecha = ceiling_date(fecha, '1 month') -1,
         typeCrime = as.double(crime)) %>% 
  filter(typeCrime != 2000) %>% 
  select(inegiEntidad = state_code, 
         inegiEntidadName = edo_nom,
         typeCrimeName = crime1,
         typeCrime,
         date = fecha,
         researchFoldersRate ,
         researchFolders,
         victimCase,
         victimRate)

c_codes <- virk_est_new %>% distinct(typeCrime,
                                     typeCrimeName)



virk_est_old <- old_w_robo %>% 
  left_join(edo_code_join, by = c('state_code' = 'edo_code')) %>% 
  left_join(c_codes, by = c('crime1' = 'typeCrimeName')) %>% 
  mutate(victimCase = as.double(NA),
         victimRate = as.double(NA),
         fecha = ceiling_date(fecha, '1 month') -1,
         state_code = as.double(state_code)) %>% 
  select(inegiEntidad = state_code, 
         inegiEntidadName = edo_nom,
         typeCrimeName = crime1,
         typeCrime,
         date = fecha,
         researchFoldersRate = tasa,
         researchFolders = absolutos,
         victimCase,
         victimRate)

virk_est_sec <- b_sec %>% 
  mutate(typeCrime = 2000,
         typeCrimeName = 'Secuestro') %>% 
  select(inegiEntidad = state_code, 
         inegiEntidadName = Entidad ,
         typeCrimeName,
         typeCrime,
         date = fecha,
         researchFoldersRate,
         researchFolders,
         victimCase,
         victimRate)  

no_dels <- c('Homicidio doloso con arma de fuego',
             'Lesiones dolosas con arma de fuego',
             'Robo en espacio o vía pública',
             'Homicidio doloso sin Fem',
             'Homicidio doloso con arma de fuego sin Fem',
             'Feminicidio con arma de fuego',
             'Robo en transporte')

virk_final <- rbind(virk_est_new,
                    virk_est_sec,
                    virk_est_old) %>% 
  mutate(typeCrime = ifelse(typeCrimeName == 'Secuestro', 2000, typeCrime)) %>% 
  filter(no_in(typeCrimeName, no_dels)) %>% 
  arrange(date)

table(virk_final$date)

# ####write virk
# 

virk_path <- paste0(output, '/estatal_virk_ordenado.csv')

load <- read_csv(virk_path, col_types = c(victimCase = 'd',
                                           victimRate = 'd'))

virk_final %>% anti_join(load)



virk_final %>%
  write_csv(path = virk_path)

