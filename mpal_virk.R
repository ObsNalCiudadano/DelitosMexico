###############################################
## Procesamiento mensual de datos municipales #
## Autor: Erik & Dani                         #
###############################################

#cargar paquetes
library(tidyverse)
library(writexl)
library(haven)
library(lubridate)
library(dtplyr)
library(jsonlite)
library(httr)



#definir data frame de meses
aux_m <- data.frame(
  num_mes = c(-1:12),  
  
  nom_mes = c("nov","dic","ene", "feb", "mar", "abr", "may", "jun",
              "jul", "ago", "sep", "oct", "nov", "dic"))



#definir mes actual
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


# directorios
output <- 'C:/Users/emcdo/Google Drive/big_carpet/Bases Generales/tidy'


histtas <- read_csv('C:/Users/emcdo/OneDrive/Documents/hist_mun_tas.csv')

# cargar base municipal historica
histabsm <- read_csv('C:/Users/emcdo/OneDrive/Documents/hist_mun.csv') %>% 
  inner_join(histtas, by = c('state_code', 'mun_code', 'municip', 
                             'categoryid', 'typeid', 'subtypeid'))
  


#lista de municipios
munlist <- histabsm %>% select(mun_code, municip) %>% distinct(mun_code, municip)#generar lista completa de municipios


#cargar datos actualizados
mun_2 <- read_dta('https://www.dropbox.com/s/qva8a3mcabc2tmw/mastermpal.dta?dl=1',
                  col_select = c(state_code:mes_abs, t_ene2015:mes_tasa, crime)) %>%
  left_join(munlist, by = 'mun_code') %>%
  mutate(mun_code = as.double(mun_code))






print('master cargado')

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

mun1labs <- histabsm %>%
  inner_join(categoryidlab, by = 'categoryid') %>%
  inner_join(typelab, by = 'typeid') %>%
  inner_join(subtypelab, by = 'subtypeid') %>%
  mutate(categoryid = categoryid1,
         typeid = typeid1,
         subtypeid = subtypeid1) %>%
  select(-categoryid1, -typeid1, -subtypeid1)


old_m <- rbind(
  
  mun1labs %>%
    filter(categoryid == 'INCIDENCIA DELICTIVA',
           typeid == 'TOTAL',
           subtypeid == 'TOTAL') %>%
    mutate(crimeid = 'Total de delitos'),
  
  mun1labs %>%
    filter(categoryid == 'DELITOS SEXUALES (VIOLACION)',
           typeid == 'VIOLACION',
           subtypeid == 'VIOLACION')%>%
    mutate(crimeid = 'Violacion'),
  
  mun1labs %>%
    filter(categoryid == 'HOMICIDIOS',
           typeid == 'DOLOSOS',
           subtypeid == 'CON ARMA DE FUEGO')%>%
    mutate(crimeid = 'Homicidio doloso con arma de fuego'),
  
  mun1labs %>%
    filter(categoryid == 'LESIONES',
           typeid == 'DOLOSAS',
           subtypeid == 'CON ARMA DE FUEGO')%>%
    mutate(crimeid = 'Lesiones con arma fuego'),
  
  mun1labs %>%
    filter(categoryid == 'PRIV. DE LA LIBERTAD (SECUESTRO)',
           typeid == 'SECUESTRO',
           subtypeid == 'SECUESTRO')%>%
    mutate(crimeid = 'Secuestro'),
  
  mun1labs %>%
    filter(categoryid == 'ROBO COMUN',
           typeid == 'TOTAL',
           subtypeid == 'A NEGOCIO')%>%
    mutate(crimeid = 'Robo a negocio'),
  
  mun1labs %>%
    filter(categoryid == 'ROBO COMUN',
           typeid == 'TOTAL',
           subtypeid == 'DE VEHICULOS')%>%
    mutate(crimeid = 'Robo de Vehículos'),
  
  mun1labs %>%
    filter(categoryid == 'DELITOS PATRIMONIALES',
           typeid == 'EXTORSION',
           subtypeid == 'EXTORSION')%>%
    mutate(crimeid = 'Extorsión'),
  
  mun1labs %>%
    filter(categoryid == 'HOMICIDIOS',
           typeid == 'DOLOSOS',
           subtypeid == 'TOTAL')%>%
    mutate(crimeid = 'Homicidio doloso'),
  
  mun1labs %>%
    filter(categoryid == 'HOMICIDIOS',
           typeid == 'CULPOSOS',
           subtypeid == 'TOTAL')%>%
    mutate(crimeid = 'Homicidio culposo'),
  
  mun1labs %>%
    filter(categoryid == 'LESIONES',
           typeid == 'TOTAL',
           subtypeid == 'TOTAL')%>%
    mutate(crimeid = 'Lesiones dolosas total'),
  
  mun1labs %>%
    filter(categoryid == 'ROBO COMUN',
           typeid == 'TOTAL',
           subtypeid == 'A CASA HABITACION')%>%
    mutate(crimeid = 'Robo a casa habitación'),
  
  mun1labs %>%
    filter(categoryid == 'ROBO COMUN',
           typeid == 'TOTAL',
           subtypeid == 'A TRANSEUNTES')%>%
    mutate(crimeid = 'Robo a transeúnte'),
  
  
  mun1labs %>%
    filter(categoryid == 'ROBO TOTAL',
           typeid == 'CON VIOLENCIA',
           subtypeid == 'TOTAL')%>%
    mutate(crimeid = 'Robo total con violencia')
  
) %>%
  select(-categoryid, -typeid, -subtypeid)


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

print('op. columnas listo')

old2pre <- inner_join(old_m, alt_list, by = 'crimeid') %>%
  mutate(crimeid = crime1) %>%
  select(-crime1)


robotrans <- mun1labs %>%
  filter(categoryid == 'ROBO COMUN',
         typeid == 'TOTAL',
         subtypeid == 'A TRANSEUNTES')%>%
  mutate(crimeid = 'Robo a transeúnte total') %>%
  select(-categoryid, -typeid, -subtypeid)

old2 <- rbind(old2pre, robotrans)



#######################################Limpiar las claves de la base nueva
labtib <- tribble(
  ~crime, ~crimeid,
  0,                            'Total de delitos',
  1100,                           'Homicidio doloso',
  1101,         'Homicidio doloso con arma de fuego',
  1110,                   'Homicidio doloso sin Fem',
  1111, 'Homicidio doloso con arma de fuego sin Fem',
  1120,                                'Feminicidio',
  1121,              'Feminicidio con arma de fuego',
  1200,                          'Homicidio culposo',
  2000,                                  'Secuestro',
  3000,                                  'Extorsión',
  4001,                         'Robo con violencia',
  4100,                           'Robo de vehículo',
  4200,                     'Robo a casa habitación',
  # 4300,                            'Robo de negocio',
  4300,                            'Robo a negocio',
  4400,                    'Robo a transeúnte total',
  4410,              'Robo en espacio o vía pública',
  4420,                         'Robo en transporte',
  4940, "Robo en transporte público",
  5000,                                  'Violación',
  6000,                         'Violencia familiar',
  7000,                          'Trata de personas',
  8000,                               'Narcomenudeo',
  9000,                           'Lesiones dolosas',
  9001,         'Lesiones dolosas con arma de fuego')



##gathereo historico




old_gather <- old2 %>% 
  gather(ene2011:tdic2014, key = 'periodo', value = 'valor')

old_mun_gath_abs <- old_gather %>% 
  filter(!str_detect(periodo, 't') | (str_detect(periodo, 'oct') & !(str_detect(periodo, 'toct')))) %>% 
  select(mun_code, periodo, crimeid, abs = valor)

mun_gath_old <- old_gather %>% 
    filter(str_detect(periodo, 't'),
           str_sub(periodo, 1,3) !='oct') %>% 
    mutate(tasa = valor,
           periodo = str_remove(periodo, 't')) %>% 
    select(-c(valor)) %>% 
    inner_join(old_mun_gath_abs, by = c('mun_code', 'periodo', 'crimeid')) %>% 
    mutate(nom_mes = str_sub(periodo, 1, 3),
           ao = str_sub(periodo, -4, -1)) %>% 
    left_join(aux_m[3:14,], by = 'nom_mes') %>% 
    mutate(fecha = ceiling_date(ymd(paste(ao, num_mes, '01', sep = '-')), '1 month')-1) %>% 
    select(-c(num_mes, nom_mes, ao))




##gathereo new
mun2joined <- inner_join(mun_2, labtib, by = 'crime')%>%
  select(state_code:mes_tasa, crime, municip, crimeid) %>%
  select(-crime)

mun_gather_new <- mun2joined %>% 
  gather(ene2015:mes_tasa, key = 'periodo', value = 'valor') 



mun_gath_abs <- mun_gather_new %>% 
  filter(!str_detect(periodo, 't_')) %>% 
  select(mun_code, periodo, crimeid, abs = valor)

system.time(
mun_gath_tas <- mun_gather_new %>% 
  filter(str_detect(periodo, 't_')) %>% 
  mutate(tasa = valor,
         periodo = str_remove(periodo, 't_')) %>% 
  select(-c(valor, mun_num)) %>% 
  inner_join(mun_gath_abs, by = c('mun_code', 'periodo', 'crimeid')) %>% 
  mutate(nom_mes = str_sub(periodo, 1, 3),
         ao = str_sub(periodo, -4, -1)) %>% 
  left_join(aux_m[3:14,], by = 'nom_mes') %>% 
  mutate(fecha = ceiling_date(ymd(paste(ao, num_mes, '01', sep = '-')), '1 month')-1) %>% 
  select(-c(num_mes, nom_mes, ao))
)

mun_fin <- rbind(mun_gath_tas, mun_gath_old) %>% 
  mutate(Crime = crimeid) %>% 
  select(-c(periodo, crimeid))





###########################virk
input <- paste0("C:/Users/emcdo/Google Drive/big_carpet/Bases Generales/tidy")
output <- input
delitos <- c("Homicidio doloso", "Feminicidio", "Homicidio culposo", "Extorsión", "Secuestro", 
             "Robo con violencia", "Robo de vehículo", "Robo a casa habitación", "Robo a negocio", "Robo a transeúnte total",
             "Robo en transporte público", "Violación", "Violencia familiar", "Trata de personas", "Narcomenudeo", "Lesiones dolosas")



b_comp <- mun_fin 


b_edo_code <- read_csv(paste(output, "estados.csv", sep = "/"))
names(b_edo_code) <- c("state_code", "estado")

b_comp_2 <- b_comp %>% left_join(b_edo_code, by = "state_code") %>% 
  filter(Crime %in% delitos)


b_crime_code <- read_csv(paste(input, "crime_code.csv", sep = "/")) %>% 
  mutate(Crime = crime) %>% select(-crime)

Encoding(b_crime_code$Delitos) <- 'latin1'                              

b_crime_code <- b_crime_code %>% 
  rbind(data.frame(Crime = 4940,
                   Delitos = 'Robo en transporte público'))




b_comp_3 <- b_comp_2 %>% left_join(b_crime_code, by = c("Crime" = "Delitos")) %>% 
  select(state_code, mun_code, estado, municip, Crime, fecha, tasa, abs, Crime.y) %>% 
  mutate(municip = str_to_title(municip))




names(b_comp_3) <- c("inegiEntidad", "inegiMunicipality", "inegiEntidadName", "inegiMunicipalityName", "typeCrimeName",
                     "date", "researchFoldersRate", "researchFolders", "typeCrime")



b_comp_3 %>% 
write_csv(paste0(output, '/tidy_mpal_full.csv'))

###########################PRUEBAS###################

mpal <- read_csv(paste0(output, '/tidy_mpal_full.csv'))


b_comp_3 %>% 
  anti_join(mpal)

length(unique(b_comp_3$inegiMunicipality))

table(b_comp_3$date)

b_comp_3 %>% count(date) %>% count(n)

length(unique(b_comp_3$typeCrimeName))

range(mpal$date)
range(b_comp_3$date)

#########################################################
#########################################################
#########################################################
#########################################################
#########################################################
#########################################################
#########################################################
#########################################################
#########################################################
#########################################################
#########################################################
#########################################################
#########################################################
#########################################################







