#---------- IMPORTER LES PACKAGES ----------

library(jsonlite) #.json
library(arrow) #.parquet
library(DBI) #.sqlite
library(RSQLite) #.sqlite
library(stringr)
library(dplyr)


#---------- QUESTION 1 ----------

## Création du dossier "data/" à la racine du projet pour stocker les fichiers décompressés.
## Ce dossier est utilisé pour contenir des fichiers de données temporaires ou volumineux.

## Ajout de "data/" dans le fichier .gitignore pour éviter de versionner les fichiers de données.
## Cela permet :
## - D'éviter d'alourdir le dépôt Git avec des fichiers volumineux.
## - De protéger d'éventuelles données sensibles.
## - De ne pas versionner des fichiers temporaires ou générés dynamiquement.

## Attention : Si des fichiers du dossier "data/" doivent être partagés, il faut utiliser une autre méthode 
## comme un stockage cloud ou un autre dépôt spécifique aux données.





#---------- QUESTION 2 ----------

df_DEC_DEP <- read_json("data/DEC_DEP.json")

df_DEC_COM_Meta <- arrow::read_parquet("data/DEC_COM_Meta.parquet")

df_DEC_DEP_Meta <- arrow::read_parquet("data/DEC_DEP_Meta.parquet")

conn <- dbConnect(RSQLite::SQLite(), "data/DEC_COM.sqlite")
dbListTables(conn)
df_DEC_COM <- dbReadTable(conn = conn, name = "DEC_COM")





#---------- QUESTION 3 ----------

#----- Moins de 30 ans -----

# Commande de base

sum(grepl("AGE1", df_DEC_COM_Meta$COD_VAR)) #AGE1 = - 30 ans


# Commande CM

sum(str_detect(df_DEC_COM_Meta$COD_VAR, "AGE1"))



#----- Couples avec enfants -----

sum(str_detect(df_DEC_COM_Meta$COD_VAR, "TME[3-9]"))



#----- Indice de GINI -----

df_DEC_COM_Meta$COD_VAR[str_detect(df_DEC_COM_Meta$COD_VAR, "GI20") ]





#---------- QUESTION 4 ----------

#----- Convertir RD texte en numérique -----

df_DEC_DEP <- lapply(df_DEC_DEP, function(df) {
  
  # Remplacer la virgule par un point et convertir en numérique
  df$RD <- gsub(",", ".", df$RD)  # Remplacer la virgule par un point
  df$RD <- as.numeric(df$RD)
  
  # Retourner le dataframe avec les RD convertis
  return(df)
})



#----- Grouper les valeurs de RD et faire ressortir min/max-----

rd_dep <- sapply(df_DEC_DEP, function(x) x$RD)

rd_dep_max <- which.max(rd_dep)

rd_dep_min <- which.min(rd_dep)



#----- Lister les codes géographiques -----

dep <- df_DEC_DEP_Meta |> 
  filter(str_detect(COD_VAR, "CODGEO")) |> 
  select(LIB_MOD, COD_MOD)

dep$LIB_MOD[rd_dep_max]

dep$LIB_MOD[rd_dep_min]




#---------- QUESTION 5 ----------

str(df_DEC_COM)

## Tout est en caractère (chr).

## La modalité "s" indique que la valeur est censurée ou masquée pour des 
## raisons de confidentialité (par exemple, pour éviter d'identifier des 
## individus ou groupes spécifiques). Elle est souvent utilisée dans les données 
## publiques pour respecter les seuils de confidentialité statistique.





#---------- QUESTION 6 ----------

#----- Extraire les lignes correspondant à Nantes et Saint-Pierre -----

com <- df_DEC_COM_Meta |> 
  filter(str_detect(COD_VAR, "CODGEO")) |> 
  select(LIB_MOD, COD_MOD)

codes_nts_sp <- com[com$LIB_MOD %in% c("Nantes", "Saint-Pierre"),]

df_DEC_COM_nts_sp <- df_DEC_COM |> 
  filter(CODGEO %in% codes_nts_sp$COD_MOD)



#----- Convertir en numérique -----

df_DEC_COM_nts_sp[] <- lapply(df_DEC_COM_nts_sp, function(x) {
  
  if(is.character(x)) gsub(",", ".", x) 
  
  else x
  
})


df_DEC_COM_nts_sp[] <- lapply(df_DEC_COM_nts_sp, function(x) {
  
  as.numeric(as.character(x))
  
})





#---------- QUESTION 7 ----------

df_DEC_COM_nts_sp$Q320 - df_DEC_COM_nts_sp$Q120





#---------- QUESTION 8 ----------


df_DEC_COM$COD_DEP <- ifelse(substr(df_DEC_COM$CODGEO, 1, 2) == "97", 
                             substr(df_DEC_COM$CODGEO, 1, 3),
                             substr(df_DEC_COM$CODGEO, 1, 2))


df_DEC_COM_avec_dep <- df_DEC_COM |>
  left_join(dep, by = c("COD_DEP" = "COD_MOD"))