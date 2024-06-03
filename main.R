require(RMySQL)
require(dplyr) # nolint

# MySQL Server  - MEAPPSERVER
# OpenMRS  - Configuracao de variaveis de conexao BD OpenMRS
db_host <- Sys.getenv("MYSQL_HOST")
db_port <- as.integer(Sys.getenv("MYSQL_PORT"))
db_name <- "xipamanine" # nolint
db_user <- Sys.getenv("MYSQL_USER")
db_password <- Sys.getenv("MYSQL_PASSWORD")


vec_us_names  <- c("1_junho","1_maio","albasine","altomae","chamanculo" ,"hulene","josemacamo_cs" 
                   ,"malhangalene","mavalane_cs","maxaquene","pescadores","cimento","porto","romao","xipamanine")

source(file = 'generic_functions.R')
load(file = 'ccs_org_units.RData' )

df_muzima_cohort <- data.frame()

# Clear Log Files
file_conn <- file("log.txt", "w")
writeLines(character(0), file_conn)
close(file_conn)

for (us in vec_us_names) {
  
  db_name <- us
  message("Processando dados do CS: " , us)
  
  write_log(log_message = paste0( "-- CS ", us))
  write_log(log_message = paste0( "-- -------------------------------------------------------------------------------------------------------------------------------------------- "),)
  

  # sql_muzima_chohort_1 is defined in generic_functions.R
  tryCatch({
    # Objecto de conexao com a bd openmrs
    con_openmrs <- dbConnect(MySQL(), user = db_user, password = db_password, dbname = db_name, host = db_host, port = db_port) # nolint
    encoding <- if(grepl(pattern = 'utf8|utf-8',x = Sys.getlocale(),ignore.case = T)) 'utf8' else 'latin1'
    dbGetQuery(con_openmrs,paste("SET names",encoding))
    dbGetQuery(con_openmrs,paste0("SET SESSION character_set_server=",encoding))
    dbGetQuery(con_openmrs,paste0("SET SESSION character_set_database=",encoding))
    
    if( class(con_openmrs) !="MySQLConnection"){ break}
    
    location_id  <- fnGetOpenMRSLocationID(con_openmrs)
    
                                                                    #(monitoringDate, startDate, location , allocationDate)
    df_tmp_cohort_1 <- dbGetQuery(con_openmrs, fnGetMuzimaCohortQuery(Sys.Date(), "2024-01-01", location_id, Sys.Date() ) )
 
    
    if(nrow(df_tmp_cohort_1)  > 0 & is.numeric(location_id)) {
      df_tmp_cohort_1$us <- db_name
      df_tmp_cohort_1$distrito <- get_distrito(db_name)
      df_muzima_cohort <- plyr::rbind.fill(df_muzima_cohort,df_tmp_cohort_1)
    } else {
      message(" df_tmp_cohort_1  is empty")
    }
    write_log(log_message = paste0( "-- -------------------------------------------------------------------------------------------------------------------------------------------- "))
    dbDisconnect(conn = con_openmrs)
    rm(con_openmrs)
  }, error = function(e) {
    message("An error occurred: ", e)
    write_log(paste0("An error occurred: ", e ))
    dbDisconnect(conn = con_openmrs)
    rm(con_openmrs)
  })
  
  if(nrow(df_muzima_cohort) >0) {
    # First recreate muzma table and insert data
    
    
    con_muzima <- dbConnect(MySQL(), user = db_user, password = db_password, dbname = "muzima", host = db_host, port = db_port,  encoding = "UTF-8") # nolint
    dbGetQuery(conn = con_muzima,statement = "drop table if exists index_case;")
    dbGetQuery(conn = con_muzima,statement = sql_muzima_cohort_table)
    
    df_muzima_cohort$periodo_alocacao <- df_muzima_cohort$allocationDate  %>% sapply(FUN = get_period)
    dbWriteTable(con_muzima, "index_case", df_muzima_cohort, overwrite = FALSE, append = TRUE, row.names = FALSE)
    dbDisconnect(con_muzima)
    rm(con_muzima)
  }

}
  