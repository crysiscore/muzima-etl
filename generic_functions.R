# Define a function to write logs to a text file
write_log <- function(log_message, log_file = "log.txt") {
  # Get the current timestamp
  timestamp <- format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
  
  
  # Append the log entry to the log file
  cat(paste0(timestamp, " " ,log_message),"\n", file = log_file, append = TRUE)
}



fnGetOpenMRSLocationID <- function(con.openmrs){
  
    df_location <- dbGetQuery(conn = con.openmrs, "select  l.location_id FROM location l where l.name = (select property_value from global_property where property='default_location')")

    return(df_location$location_id[1])
    }


get_period <- function(date) {
  
  if(is.na(date)){
    return(date)
  } else {
    
    date <- as.Date(date)
    month_start <- as.Date(format(date, "%Y-%m-01"))
    if (date >= as.Date(format(date, "%Y-%m-21"))) {
      period <- format(date + 30, "%Y%m") 
    } else {
      period <- format(month_start, "%Y%m")
    }
    return(period)
  }

}



get_distrito <- function(us.name){
  
  distrito <- CCS_DATA_EXCHANGE_ORG_UNITS$orgunit_parent[which(CCS_DATA_EXCHANGE_ORG_UNITS$us_names==us.name)]
  
  distrito
}



fnGetMuzimaCohortQuery <- function(monitoringDate, startDate, location , allocationDate){
  
  

sql_muzima_chohort_1 <- paste0("select *
from 
(
   select   casoIndex.*,
         timestampdiff(year,personContact.birthdate,curdate()) personContactAge,
         concat(ifnull(pn.given_name,''),' ',ifnull(pn.middle_name,''),' ',ifnull(pn.family_name,'')) as personContactName,
         concat(ifnull(pnIndex.given_name,''),' ',ifnull(pnIndex.middle_name,''),' ',ifnull(pnIndex.family_name,'')) as personIndexName,
         personContact.person_id personContactId,
         personContact.gender personContactGender,    
         personIndex.gender personIndexGender,     
         timestampdiff(year,personIndex.birthdate,curdate()) personIndexAge,
         pidIndex.identifier as nidIndex,
         CONCAT_WS('; ', pad3.address6, pad3.address5, pad3.address1) as indexAddress,
         pat.value telefone
         
         
   from 

   (
      Select   indexSource.*,
            obsTeste.value_coded hivTestResultId,
            case obsTeste.value_coded        
               when 703 then 'Positivo'
               when 664 then 'Negativo'
               when 1067 then 'Desconhecido'
               when 1065 then 'Sim'
               when 1066 then 'Não'          
            else null end as hivTestResult,
            date(obsTeste.obs_datetime) hivTestDate,
            date(obsAllocation.allocationDate) allocationDate,
            obsAllocation.allocatedToUserName,
            obsAllocation.allocatedToPersonName,
            obsTesteOfered.testOferred,
            obsTesteOfered.obsTesteOferedDate,
            obsTesteAccepted.testAccepted,
            obsTesteAccepted.obsTesteAcceptedDate,
            obsNid.nidContact,
            obsTesteAfter.value_coded hivTestResultIdAfter,
            if(obsTesteAfter.value_coded=703,'Positivo',if(obsTesteAfter.value_coded=664,'Negativo',if(obsTesteAfter.value_coded=1067,'Desconhecido',''))) hivTestResultAfter,
            date(obsTesteAfter.obs_datetime) hivTestDateAfter,
            date(obsArtStart.value_datetime) artStartDate,
            (SELECT GROUP_CONCAT(DISTINCT(case o.value_coded
                                       when 1366 then 'Óbito'
                                       when 2024 then 'Endereço errado'
                                       when 2026 then 'Mudou de casa'
                                       when 2011 then 'Viajou'
                                       when 2032 then 'Outros motivos'
                                    else null end )) as reason
            FROM obs o 
            WHERE o.voided = 0 AND o.person_id=indexSource.conviventePersonId AND (DATE(o.obs_datetime) BETWEEN DATE(obsAllocation.allocationDate) AND DATE('",monitoringDate,"'))) AS notFoundReason,
            obsReferredHF.referredHFName
            
      from 
      (
         select   aceitaindex.patient_id,
               aceitaindex.data_aceita,
               aceitaindex.data_testagem,
               aceitaindex.local_teste,
               aceitaindex.indexOrigemCi,
               rs.person_a conviventePersonId,
               rs.relationship,
               rst.a_is_to_b relacionamentoComIndice
         from 
         (  
            select   ultimoIndex.patient_id,
                  max(ultimoIndex.data_aceita) data_aceita,
                  max(obsdata.value_datetime) data_testagem,
                  case obsComunidade.value_coded
                     when 21154 then 'Unidade Sanitária'
                     when 6403 then 'Comunidade'
                  else null end as local_teste,
                  case obsOrigem.value_coded       
                     when 1597 then 'UATS'
                     when 5271 then 'CPF'
                     when 23876 then 'Enf. Cirurgia'
                     when 23875 then 'Enf. Medicina'
                     when 23874 then 'Enf. Pediatria'
                     when 23873 then 'Triagem Adulto'
                     when 23872 then 'Triagem Banco de Socorro'
                     when 23871 then 'Banco de Socorro'
                     when 1926 then 'Banco de Sangue'
                     when 23868 then 'Estomatologia'
                     when 6303 then 'VBG'
                     when 23870 then 'Gabinete Médico'
                     when 1596 then 'Consulta Externa'
                     when 1987 then 'SAAJ'
                     when 23878 then 'CPP'
                     when 1978 then 'CPN'
                     when 1872 then 'CCR'
                     when 1414 then 'PNCTL'
                     when 23869 then 'Maternidade'
                     when 6423 then 'CCS'
                     when 6142 then 'APSS-PP'
                  else null end as indexOrigemCi
                  
            from 
            (
               select   p.patient_id ,max(e.encounter_datetime) data_aceita            
               from  patient p 
                     inner join encounter e on p.patient_id=e.patient_id               
                     inner join encounter_type et on et.encounter_type_id=e.encounter_type               
               where    e.voided=0 and p.voided=0 and e.location_id= ",location," and et.uuid='4f215536-f90d-4e0c-81e1-074047eecd68' and Date(e.encounter_datetime)<=date('",allocationDate,"')
               group by p.patient_id
            ) ultimoIndex
            inner join encounter e on e.patient_id=ultimoIndex.patient_id
            inner join encounter_type et on et.encounter_type_id=e.encounter_type   
            inner join obs obsComunidade on e.encounter_id=obsComunidade.encounter_id
            left join obs obsdata on e.encounter_id=obsdata.encounter_id and obsdata.concept_id=23879 and obsdata.voided=0
            left join obs obsOrigem on e.encounter_id=obsOrigem.encounter_id and obsOrigem.concept_id=23877 and obsOrigem.voided=0
            where    obsComunidade.voided=0 and e.voided=0 and obsComunidade.concept_id=21155 and obsComunidade.value_coded IN (21154,6403) and 
                  e.location_id=",location, " and et.uuid='4f215536-f90d-4e0c-81e1-074047eecd68' and Date(e.encounter_datetime)<=date('",allocationDate,"') and 
                  ultimoIndex.data_aceita=e.encounter_datetime
            group by ultimoIndex.patient_id        
         ) aceitaindex
         inner join relationship rs on rs.person_b=aceitaindex.patient_id and rs.relationship in (6,12,13,14,21,3)
         inner join relationship_type rst on rst.relationship_type_id=rs.relationship
         
         union 
         
         select   aceitaindex.patient_id,
               aceitaindex.data_aceita,
               aceitaindex.data_testagem,
               aceitaindex.local_teste,
               aceitaindex.indexOrigemCi,
               rs.person_b conviventePersonId,
               rs.relationship,
               rst.b_is_to_a relacionamentoComIndice
         from 
         (  
            select   ultimoIndex.patient_id,
                  max(ultimoIndex.data_aceita) data_aceita,
                  max(obsdata.value_datetime) data_testagem,
                  case obsComunidade.value_coded
                     when 21154 then 'Unidade Sanitária'
                     when 6403 then 'Comunidade'
                  else null end as local_teste,
                  case obsOrigem.value_coded       
                     when 1597 then 'UATS'
                     when 5271 then 'CPF'
                     when 23876 then 'Enf. Cirurgia'
                     when 23875 then 'Enf. Medicina'
                     when 23874 then 'Enf. Pediatria'
                     when 23873 then 'Triagem Adulto'
                     when 23872 then 'Triagem Banco de Socorro'
                     when 23871 then 'Banco de Socorro'
                     when 1926 then 'Banco de Sangue'
                     when 23868 then 'Estomatologia'
                     when 6303 then 'VBG'
                     when 23870 then 'Gabinete Médico'
                     when 1596 then 'Consulta Externa'
                     when 1987 then 'SAAJ'
                     when 23878 then 'CPP'
                     when 1978 then 'CPN'
                     when 1872 then 'CCR'
                     when 1414 then 'PNCTL'
                     when 23869 then 'Maternidade'
                     when 6423 then 'CCS'
                     when 6142 then 'APSS-PP'
                  else null end as indexOrigemCi
                  
            from 
            (
               select   p.patient_id ,max(e.encounter_datetime) data_aceita            
               from  patient p 
                     inner join encounter e on p.patient_id=e.patient_id               
                     inner join encounter_type et on et.encounter_type_id=e.encounter_type               
               where    e.voided=0 and p.voided=0 and e.location_id=",location, " and et.uuid='4f215536-f90d-4e0c-81e1-074047eecd68' and Date(e.encounter_datetime)<='",allocationDate,"'
               group by p.patient_id
            ) ultimoIndex
            inner join encounter e on e.patient_id=ultimoIndex.patient_id
            inner join encounter_type et on et.encounter_type_id=e.encounter_type   
            inner join obs obsComunidade on e.encounter_id=obsComunidade.encounter_id
            left join obs obsdata on e.encounter_id=obsdata.encounter_id and obsdata.concept_id=23879 and obsdata.voided=0
            left join obs obsOrigem on e.encounter_id=obsOrigem.encounter_id and obsOrigem.concept_id=23877 and obsOrigem.voided=0
            where    obsComunidade.voided=0 and e.voided=0 and obsComunidade.concept_id=21155 and obsComunidade.value_coded IN (21154,6403) and 
                  e.location_id=",location, " and et.uuid='4f215536-f90d-4e0c-81e1-074047eecd68' and Date(e.encounter_datetime)<='",allocationDate,"' and 
                  ultimoIndex.data_aceita=e.encounter_datetime
            group by ultimoIndex.patient_id  
         ) aceitaindex
         inner join relationship rs on rs.person_a=aceitaindex.patient_id and rs.relationship in (6,12,13,14,21,3)
         inner join relationship_type rst on rst.relationship_type_id=rs.relationship  
         
      ) indexSource
      left join 
      (
         select lastTest.person_id,max(lastTest.obs_datetime) obs_datetime,obs.value_coded
         from 
         (
            select   person_id,max(obs_datetime) obs_datetime
            from  obs 
            where    voided=0 and concept_id in (23779,23780) and date(obs_datetime) < Date('", startDate, "')
            group by person_id
         ) lastTest
         inner join obs on obs.person_id=lastTest.person_id 
         where    date(lastTest.obs_datetime)=date(obs.obs_datetime) and 
               obs.voided=0 and obs.concept_id in (23779,23780) and obs.value_coded in (703,1067,664)
         group by lastTest.person_id
         
      ) obsTeste on obsTeste.person_id=indexSource.conviventePersonId 
      left join 
      (
         select lastAlocation.person_id,max(lastAlocation.obs_datetime) allocationDate,users.username allocatedToUserName,
               concat(ifnull(person_name.given_name,''),' ',ifnull(person_name.middle_name,''),' ',ifnull(person_name.family_name,'')) as allocatedToPersonName
         from 
         (
            select   person_id,max(obs_datetime) obs_datetime
            from  obs 
            where    voided=0 and concept_id=1912 and obs.encounter_id is null and comments is null and (date(obs_datetime)>='", startDate, "' and date(obs_datetime)<='",monitoringDate,"' )
                  and not EXISTS (SELECT * FROM obs o where o.obs_group_id = obs.obs_id)
            group by person_id
         ) lastAlocation
         inner join obs on obs.person_id=lastAlocation.person_id
         inner join users on users.system_id=obs.value_text
         inner join person_name on users.person_id=person_name.person_id
         where    date(lastAlocation.obs_datetime)=date(obs.obs_datetime) and 
               obs.voided=0 and obs.encounter_id is null and obs.concept_id=1912 and comments is null
         group by lastAlocation.person_id 
      ) obsAllocation on obsAllocation.person_id=indexSource.patient_id
      left join 
      (
         select lastTestOfered.person_id,max(lastTestOfered.obs_datetime) obsTesteOferedDate,
         if(obs.value_coded=1065,'Sim','Não') testOferred
         from 
         (
            select   person_id,max(obs_datetime) obs_datetime
            from  obs 
            where    voided=0 and concept_id=165266 and date(obs_datetime) BETWEEN date('", startDate, "') AND date('",monitoringDate,"')
            group by person_id
         ) lastTestOfered
         inner join obs on obs.person_id=lastTestOfered.person_id 
         where    date(lastTestOfered.obs_datetime)=date(obs.obs_datetime) and 
               obs.voided=0 and obs.concept_id=165266
         group by lastTestOfered.person_id   
      ) obsTesteOfered on obsTesteOfered.person_id=indexSource.conviventePersonId 
      left join 
      (
         select lastTestAccepted.person_id,max(lastTestAccepted.obs_datetime) obsTesteAcceptedDate,
         if(obs.value_coded=1065,'Sim','Não') testAccepted
         from 
         (
            select   person_id,max(obs_datetime) obs_datetime
            from  obs 
            where    voided=0 and concept_id=165267 and date(obs_datetime) BETWEEN date('", startDate, "') AND date('",monitoringDate,"')
            group by person_id
         ) lastTestAccepted
         inner join obs on obs.person_id=lastTestAccepted.person_id 
         where    date(lastTestAccepted.obs_datetime)=date(obs.obs_datetime) and 
               obs.voided=0 and obs.concept_id=165267
         group by lastTestAccepted.person_id 
      ) obsTesteAccepted on obsTesteAccepted.person_id=indexSource.conviventePersonId 
      left join 
      (
         select lastNid.person_id,obs.value_text nidContact
         from 
         (
            select   person_id,max(obs_datetime) obs_datetime
            from  obs 
            where    voided=0 and concept_id=23781 and date(obs_datetime) BETWEEN date('", startDate, "') AND date('",monitoringDate,"')
            group by person_id
         ) lastNid
         inner join obs on obs.person_id=lastNid.person_id 
         where    date(lastNid.obs_datetime)=date(obs.obs_datetime) and 
               obs.voided=0 and obs.concept_id=23781
         group by lastNid.person_id 
      ) obsNid on obsNid.person_id=indexSource.conviventePersonId 
      left join 
      (
         select lastTest.person_id,max(lastTest.obs_datetime) obs_datetime,obs.value_coded
         from 
         (
            select   person_id,max(obs_datetime) obs_datetime
            from  obs 
            where    voided=0 and concept_id in (23779,23780) and date(obs_datetime)<='",monitoringDate,"'
            group by person_id
         ) lastTest
         inner join obs on obs.person_id=lastTest.person_id 
         where    date(lastTest.obs_datetime)=date(obs.obs_datetime) and 
               obs.voided=0 and obs.concept_id in (23779,23780) and obs.value_coded in (703,1067,664)
         group by lastTest.person_id   
      ) obsTesteAfter on obsTesteAfter.person_id=indexSource.conviventePersonId and ((date(obsTesteAfter.obs_datetime)>date(obsTeste.obs_datetime)) or obsTeste.obs_datetime is null)
      left join 
      (
         select obs.person_id,obs.value_datetime
         from 
         (
            select   person_id,value_datetime,max(obs_datetime) obs_datetime
            from  obs 
            where    voided=0 and concept_id=1190 and date(obs_datetime)<='",monitoringDate,"'
            group by person_id
         ) latestArtStartObs
         inner join obs on obs.person_id=latestArtStartObs.person_id 
         where    date(latestArtStartObs.obs_datetime)=date(obs.obs_datetime) and 
               obs.voided=0 and obs.concept_id=1190
         group by latestArtStartObs.person_id   
      ) obsArtStart on obsArtStart.person_id=indexSource.patient_id
      left join 
      (
         select notFound.person_id,obs.value_coded
         from 
         (
            select   person_id,max(obs_datetime) obs_datetime
            from  obs 
            where    voided=0 and concept_id=2031 and date(obs_datetime) BETWEEN date('", startDate, "') AND date('",monitoringDate,"')
            group by person_id
         ) notFound
         inner join obs on obs.person_id=notFound.person_id 
         where    date(notFound.obs_datetime)=date(obs.obs_datetime) and 
               obs.voided=0 and obs.concept_id=2031
         group by notFound.person_id 
      ) notFoundObs on notFoundObs.person_id=indexSource.conviventePersonId
      left join 
      (
         select lastReferredHF.person_id,obs.value_text referredHFName
         from 
         (
            select   person_id,max(obs_datetime) obs_datetime
            from  obs 
            where    voided=0 and concept_id=1460 and date(obs_datetime) BETWEEN date('", startDate, "') AND date('",monitoringDate,"')
            group by person_id
         ) lastReferredHF
         inner join obs on obs.person_id=lastReferredHF.person_id 
         where    date(lastReferredHF.obs_datetime)=date(obs.obs_datetime) and 
               obs.voided=0 and obs.concept_id=1460
         group by lastReferredHF.person_id 
      ) obsReferredHF on obsReferredHF.person_id=indexSource.conviventePersonId

      
   ) casoIndex
   inner join person personIndex on personIndex.person_id=casoIndex.patient_id 
   inner join person personContact on personContact.person_id=casoIndex.conviventePersonId 
   left join 
   (        
      select pn1.*
      from person_name pn1
      inner join 
      (
         select person_id,max(person_name_id) id 
         from person_name
         where voided=0
         group by person_id
      ) pn2
      where pn1.person_id=pn2.person_id and pn1.person_name_id=pn2.id
   ) pn on pn.person_id=casoIndex.conviventePersonId  
   left join 
   (        
      select pn1.*
      from person_name pn1
      inner join 
      (
         select person_id,max(person_name_id) id 
         from person_name
         where voided=0
         group by person_id
      ) pn2
      where pn1.person_id=pn2.person_id and pn1.person_name_id=pn2.id
   ) pnIndex on pnIndex.person_id=casoIndex.patient_id   
   left join 
   (       
      select pid1.*
      from patient_identifier pid1
      inner join
      (
         select patient_id,max(patient_identifier_id) id
         from patient_identifier
         where voided=0
         group by patient_id
      ) pid2
      where pid1.patient_id=pid2.patient_id and pid1.patient_identifier_id=pid2.id
   ) pidIndex on pidIndex.patient_id=casoIndex.patient_id
   /*left join 
   (       
      select pid1.*
      from patient_identifier pid1
      inner join
      (
         select patient_id,max(patient_identifier_id) id
         from patient_identifier
         where voided=0
         group by patient_id
      ) pid2
      where pid1.patient_id=pid2.patient_id and pid1.patient_identifier_id=pid2.id
   ) pidContact on pidContact.patient_id=casoIndex.conviventePersonId*/
   left join 
   (  select pad1.*
      from person_address pad1
      inner join 
      (
         select person_id,max(person_address_id) id 
         from person_address
         where voided=0
         group by person_id
      ) pad2
      where pad1.person_id=pad2.person_id and pad1.person_address_id=pad2.id
   ) pad3 on pad3.person_id=casoIndex.patient_id   
   left join 
   person_attribute pat on pat.person_id=casoIndex.patient_id and pat.person_attribute_type_id=9 and pat.value is not null and pat.value<>'' and pat.voided=0

) finalIndex
where

   (
      (
         ((relationship in (3,6,12,13,14)) and (hivTestResultId in (1067,1066) or hivTestResultId is null) and 
         ((personIndexAge<15 and personContactAge>15) or (personIndexAge>15 and (personContactAge<15 or personContactAge is null)))) 
      )
      OR 
      (
         ((relationship in (21,9)) and (hivTestResultId in (1067,1066) or hivTestResultId is null)) 
      
      )
      OR 
      (
         ((relationship in (21,9)) and (hivTestResultId=664)) 
      
      )
   )

   AND EXISTS (SELECT * FROM cohort_member cm 
         WHERE    cm.patient_id = finalIndex.patient_id 
               AND date(cm.date_created) BETWEEN date('", startDate, "') AND date('",allocationDate,"')
               AND cm.cohort_id = 8)
group by conviventePersonId
")
    
                               sql_muzima_chohort_1
    
}


sql_muzima_cohort_table <- "
create table if not exists muzima.index_case
(
    id                      int auto_increment
        primary key,
    patient_id              int                                 null,
    data_aceita             datetime                            null,
    data_testagem           datetime                            null,
    local_teste             varchar(255)                        null,
    indexOrigemCi           varchar(255)                        null,
    conviventePersonId      int                                 null,
    relationship            int                                 null,
    relacionamentoComIndice varchar(50)                         null,
    hivTestResultId         int                                 null,
    hivTestResult           varchar(50)                         null,
    hivTestDate             date                                null,
    allocationDate          date                                null,
    allocatedToUserName     varchar(255)                        null,
    allocatedToPersonName   varchar(152)                        null,
    testOferred             varchar(3)                          null,
    obsTesteOferedDate      datetime                            null,
    testAccepted            varchar(3)                          null,
    obsTesteAcceptedDate    datetime                            null,
    nidContact              text                                null,
    hivTestResultIdAfter    int                                 null,
    hivTestResultAfter      varchar(125)                        null,
    hivTestDateAfter        date                                null,
    artStartDate            date                                null,
    notFoundReason          varchar(256)                        null,
    referredHFName          text                                null,
    personContactAge        bigint                              null,
    personContactName       varchar(152)                        null,
    personIndexName         varchar(152)                        null,
    personContactId         int                                 null,
    personContactGender     varchar(50)                         null,
    personIndexGender       varchar(50)                         null,
    personIndexAge          bigint                              null,
    nidIndex                varchar(50)                         null,
    indexAddress            text                                null,
    telefone                varchar(50)                         null,
    us                      varchar(255)                        null,
    periodo_alocacao        varchar(255)                        null,
    sync_date               timestamp default CURRENT_TIMESTAMP not null,
    distrito                 varchar(255)                      null
);

"