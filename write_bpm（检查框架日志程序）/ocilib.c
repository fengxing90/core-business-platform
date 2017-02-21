#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <oci.h>
#include "ocilib.h"
#include "public.h"

OCIEnv         *envhp;
OCIError       *errhp;
OCIServer      *srvhp;
OCISvcCtx      *svchp;
OCISession     *authp;
OCIStmt        *stmthp;
OCIStmt        *stmthp1;
OCIStmt        *stmthp2;
OCIStmt        *stmthp3;

/*********************************************************************
**
**    Function    :  logon() 
**    Description :  begin session with database and allocate resource
**    Author      :  LanHaiBo 
**
**********************************************************************/
int logon(char *username, char *password, char *dbname)
{
  sword status;
  
  /* 1  
   * create OCI environment */
  OCIEnvCreate(&envhp,
               OCI_DEFAULT,
               (dvoid *) 0,
               (dvoid * (*)(dvoid *, size_t)) 0,
               (dvoid * (*)(dvoid *, dvoid *, size_t)) 0,
               (void (*)(dvoid *, dvoid *)) 0,
               0,
               (dvoid **) 0);

  /* 2.1 
   * allocating ERROR handle */
  OCIHandleAlloc((dvoid *) envhp,
                 (dvoid **) &errhp,
                 OCI_HTYPE_ERROR,
                 (size_t) 0,
                 (dvoid **) 0);
  /* 2.2 
   * allocating SERVER handle */
  OCIHandleAlloc((dvoid *) envhp,
                 (dvoid **) &srvhp,
                 OCI_HTYPE_SERVER,
                 (size_t) 0,
                 (dvoid **) 0);
  /* 2.3 
   * allocating SERVICE CONTEXT handle */
  OCIHandleAlloc((dvoid *) envhp,
                 (dvoid **) &svchp,
                 OCI_HTYPE_SVCCTX,
                 (size_t) 0,
                 (dvoid **) 0);

  /* 3
   * connect database */
  status = OCIServerAttach(srvhp, errhp,
                           (text *)dbname, (ub4) strlen(dbname), 0);
  if (status != OCI_SUCCESS)
  {
     return -1;
  }
                 
  /* 4
   * set SERVICE CONTEXT relating SERVER property */
  OCIAttrSet((dvoid *) svchp,
             OCI_HTYPE_SVCCTX,
             (dvoid *) srvhp,
             (ub4) 0,
             OCI_ATTR_SERVER,
             errhp);
                 

  /* 5.1 
   * allocating SESSION handle */
  OCIHandleAlloc((dvoid *) envhp,
                 (dvoid **) &authp,
                 OCI_HTYPE_SESSION,
                 (size_t) 0,
                 (dvoid **) 0);
  /* 
   * 5.2 set SESSION property */
  OCIAttrSet((dvoid *) authp,
             OCI_HTYPE_SESSION,
             (text *)username,
             (ub4) strlen(username),
             OCI_ATTR_USERNAME,
             errhp);
  OCIAttrSet((dvoid *) authp,
             OCI_HTYPE_SESSION,
             (text *)password,
             (ub4) strlen(password),
             OCI_ATTR_PASSWORD,
             errhp);

  /* 6 
   * set SERVICE CONTEXT (SESSION) property */
  OCIAttrSet((dvoid *) svchp,
             OCI_HTYPE_SVCCTX,
             (dvoid *) authp,
             (ub4) 0,
             OCI_ATTR_SESSION,
             errhp);

  /* 7
   * allocating STATMENT handle */
  OCIHandleAlloc((dvoid *) envhp,
                 (dvoid **) &stmthp,
                 OCI_HTYPE_STMT,
                 (size_t) 0,
                 (dvoid **) 0);
  OCIHandleAlloc((dvoid *) envhp,
                 (dvoid **) &stmthp1,
                 OCI_HTYPE_STMT,
                 (size_t) 0,
                 (dvoid **) 0);
  OCIHandleAlloc((dvoid *) envhp,
                 (dvoid **) &stmthp2,
                 OCI_HTYPE_STMT,
                 (size_t) 0,
                 (dvoid **) 0); 
  OCIHandleAlloc((dvoid *) envhp,
                 (dvoid **) &stmthp3,
                 OCI_HTYPE_STMT,
                 (size_t) 0,
                 (dvoid **) 0);                 
             
  /* 8
   * begin session */
  status = OCISessionBegin(svchp, errhp, authp, 
                           OCI_CRED_RDBMS, (ub4) OCI_DEFAULT);
  if (status != OCI_SUCCESS)
  {
     return -1;
  }
  
  return 0;
}

/********************************************************************
**
**    Function    :  logoff()
**    Description :  end session with database and release resource
**    Author      :  LanHaiBo
**
********************************************************************/
void logoff()
{
  OCISessionEnd(svchp, errhp, authp, (ub4) 0);
  OCIServerDetach(srvhp, errhp, (ub4) OCI_DEFAULT);
  OCIHandleFree((dvoid *) envhp, OCI_HTYPE_ENV);
}


/*********************************************************************
**
**    Function    :  error_proc() 
**    Author      :  LanHaiBo 
**
**********************************************************************/
void error_proc(dvoid *errhp, sword status)
{
  text errbuf[512];
  sb4  errcode;
  
  memset(oci_err, '\0', sizeof(oci_err));  
  switch (status)
  {
    case OCI_SUCCESS:
         break;
    case OCI_SUCCESS_WITH_INFO:
         strcpy(oci_err, "OCI ERROR CODE: OCI_SUCCESS_WITH_INFO\n");
         break;
    case OCI_NEED_DATA:
         strcpy(oci_err, "OCI ERROR CODE: OCI_NEED_DATA\n");
         break;
    case OCI_NO_DATA:
         strcpy(oci_err, "OCI ERROR CODE: OCI_NO_DATA\n");
         break;
    case OCI_ERROR:
         OCIErrorGet((dvoid *) errhp, 
                     (ub4) 1, 
                     NULL,
                     &errcode,
                     errbuf, (ub4) sizeof(errbuf), 
                     OCI_HTYPE_ERROR);
         sprintf(oci_err, "OCI ERROR CODE:%d\n OCI ERROR INFO:%s\n", errcode, errbuf);
         break;
    case OCI_INVALID_HANDLE:
         strcpy(oci_err, "OCI ERROR CODE: OCI_INVALID_HANDLE\n");
         break;
    case OCI_STILL_EXECUTING:
         strcpy(oci_err, "OCI ERROR CODE: OCI_STILL_EXECUTING\n");
         break;
    default:
         strcpy(oci_err, "OCI UNKNOW ERROR\n");
         break;      
  }
  
}


/********************************************************************
**
**    Function    :  select_racip()
**    Author      :  LanHaiBo
**
********************************************************************/
int select_racip(char *table1,char *table2,char *file)
{
	  char select_statement[400],statement[400];
	  char cmd[1000];
   char   time[20];       
   char rownum[5];
   char shell[500];
   char    businessid[7];
   char    hostname[25];   
   int   ct[10];
         
	  char full_file_name[1000];
   char data_file_xml[1000];
	  FILE *file_list;

   sword     status;
   
   int  filenum=0,count=0,num=0;
    	  

    OCIDefine *def1hp, *def2hp, *def3hp, *def4hp, *def5hp, *def6hp, *def7hp, *def8hp, *def9hp;
    
     numtime(ct, -1);
      memset(time,'\0',sizeof(time));
      sprintf(time,"%04d%02d%02d%02d%02d%02d",ct[0],ct[1],ct[2],ct[3],ct[4],ct[5]);
       
     
    memset(select_statement, '\0', sizeof(select_statement));
        
    sprintf(select_statement,"select t.ip from %s t",table1);    
                          
    status = OCIStmtPrepare(stmthp, errhp,
                            select_statement, (ub4) strlen(select_statement),
                            (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);
                                                  
    status |= OCIDefineByPos(stmthp, &def1hp, errhp, 1, 
                             (ub1 *)&hostname, sizeof(hostname), SQLT_STR, 
                             (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) OCI_DEFAULT);
       

    status |= OCIStmtExecute(svchp, stmthp, errhp, (ub4) 0, (ub4) 0,
                              NULL, NULL, (ub4) OCI_DEFAULT);
 
    if (status != OCI_SUCCESS)
    {
       error_proc(errhp, status);
       memset(db_err_info, '\0', sizeof(db_err_info));
       sprintf(db_err_info, "SQL = %s\n%s", (char *)select_statement, oci_err);
       printf("%s\n", db_err_info);
       return 0;
    }
      
       
     if((file_list=fopen(file,"a")) == NULL)//打开文件 没有就创建
   	{
    	  	printf("can not create awst txt file!\n");
	  }
         
     
    while ((status = OCIStmtFetch(stmthp, errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT)) != OCI_NO_DATA)
    {
    	     
          stringtrim(hostname);
          
          memset(statement, '\0', sizeof(statement));        
          sprintf(statement,"select count(*) from %s m where m.business_id='0000000'  and (to_char(sysdate,'yyyyMMddHH24miss')-m.c_itime>1000) and  m.hostname='%s'",table2,hostname);    
                          
          status = OCIStmtPrepare(stmthp1, errhp,
                            statement, (ub4) strlen(statement),
                            (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);
                                                   
          status |= OCIDefineByPos(stmthp1, &def1hp, errhp, 1, 
                             (ub1 *)&count, sizeof(count), SQLT_INT, 
                             (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) OCI_DEFAULT);
       

         status |= OCIStmtExecute(svchp, stmthp1, errhp, (ub4) 0, (ub4) 0,
                              NULL, NULL, (ub4) OCI_DEFAULT);
 
    if (status != OCI_SUCCESS)
    {
       error_proc(errhp, status);
       memset(db_err_info, '\0', sizeof(db_err_info));
       sprintf(db_err_info, "SQL = %s\n%s", (char *)statement, oci_err);
       printf("%s\n", db_err_info);
       return -1;
    }

        
          while ((status = OCIStmtFetch(stmthp1, errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT)) != OCI_NO_DATA)
         {  
             printf("%d\n",count);  
              if(count>1)
              {
                  fprintf(file_list,"%s--ERROR--0000000--000--%s--运行不正常\n",time,hostname); 
               }
            
             num++;
          }
       filenum++;
          	  
    }
     
            	   	     	  	    
    fclose(file_list);
        
    return 0;

   

}







