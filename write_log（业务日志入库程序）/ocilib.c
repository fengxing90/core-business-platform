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

/*********************************************************************
**
**    Function    :  insert_detail_log() 
**    Author      :  FengYuXing 
**
**********************************************************************/
int insert_detail_log(char *table,char *temp1,char *temp2,char *temp3,char *temp4,char *temp5,char *temp6)
{
    char insert_statement[500],select_statement[255],delete_statement[255];
    int count=0,ret=0;
     char time[25];
     int   ct[10];
    sword  status;
    OCIDefine *def1hp;
    OCIBind   *bnd1hp, *bnd2hp, *bnd3hp, *bnd4hp, *bnd5hp, *bnd6hp, *bnd7hp, *bnd8hp,*bnd9hp, *bnd10hp, *bnd11hp, *bnd12hp;
      
      numtime(ct, -1);
      memset(time,'\0',sizeof(time));
      sprintf(time,"%4d/%d/%d %02d:%02d:%02d",ct[0],ct[1],ct[2],ct[3],ct[4],ct[5]);
     // printf("%s\n",time);

    //printf("%s %s %s %s %s %s %s\n\n",table,temp1,temp2,temp3,temp4,temp5,temp6);
    /*
    --- insert
    */

    memset(insert_statement, '\0', sizeof(insert_statement));
    sprintf(insert_statement, "insert into %s values(:1,:2,:3,:4,:5,:6)",table);

    status = OCIStmtPrepare(stmthp, errhp, 
                            insert_statement, (ub4) strlen(insert_statement),
                            (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);

    status |= OCIBindByPos(stmthp, &bnd1hp, errhp, 1,
                           (dvoid *)temp1, strlen(temp1)+1, SQLT_STR,
                           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0,
                           (ub4) 0, (ub4 *) 0, (ub4) OCI_DEFAULT);
    status |= OCIBindByPos(stmthp, &bnd2hp, errhp, 2,
                           (dvoid *)temp3, strlen(temp3)+1, SQLT_STR,
                           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0,
                           (ub4) 0, (ub4 *) 0, (ub4) OCI_DEFAULT);
    status |= OCIBindByPos(stmthp, &bnd3hp, errhp, 3,
                           (dvoid *)temp4, strlen(temp4)+1, SQLT_STR,
                           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0,
                           (ub4) 0, (ub4 *) 0, (ub4) OCI_DEFAULT);
    status |= OCIBindByPos(stmthp, &bnd4hp, errhp, 4,
                           (dvoid *)temp5, strlen(temp5)+1, SQLT_STR,
                           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0,
                           (ub4) 0, (ub4 *) 0, (ub4) OCI_DEFAULT);
    status |= OCIBindByPos(stmthp, &bnd5hp, errhp, 5,
                           (dvoid *)temp2, strlen(temp2)+1, SQLT_STR,
                           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0,
                           (ub4) 0, (ub4 *) 0, (ub4) OCI_DEFAULT);
    status |= OCIBindByPos(stmthp, &bnd6hp, errhp, 6,
                           (dvoid *)temp6, strlen(temp6)+1, SQLT_STR,
                           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0,
                           (ub4) 0, (ub4 *) 0, (ub4) OCI_DEFAULT);
    
    status |= OCIStmtExecute(svchp, stmthp, errhp, (ub4) 1, (ub4) 0,
                             (CONST OCISnapshot *) 0,
                             (OCISnapshot *) 0, 
                             (ub4) OCI_DEFAULT);
   

    if (status != OCI_SUCCESS)
    { 
    	error_proc(errhp, status);
    	printf("%s\n",oci_err); 
    	return -1;
    }
    
    
    /*
    --- transaction commit
    */
    status = OCITransCommit(svchp, errhp, 0);
    
     if (status != OCI_SUCCESS)
    { 
      error_proc(errhp, status);
      printf("%s\n",oci_err); 
      return -1;
    }
     
    return 0;
}


/*********************************************************************
**
**    Function    :  insert_collect_log() 
**    Author      :  FengYuXing 
**
**********************************************************************/
int insert_collect_log(char *table,char *temp1,char *temp2,char *temp3,char *temp4)
{
    char insert_statement[500],select_statement[255],delete_statement[255];
    int count=0,ret=0;
     char time[25];
     int   ct[10];
    sword  status;
    OCIDefine *def1hp;
    OCIBind   *bnd1hp, *bnd2hp, *bnd3hp, *bnd4hp, *bnd5hp, *bnd6hp, *bnd7hp, *bnd8hp,*bnd9hp, *bnd10hp, *bnd11hp, *bnd12hp;
      
      numtime(ct, -1);
      memset(time,'\0',sizeof(time));
      sprintf(time,"%4d/%d/%d %02d:%02d:%02d",ct[0],ct[1],ct[2],ct[3],ct[4],ct[5]);
     // printf("%s\n",time);

    /*
    --- select
    */
   
    memset(select_statement, '\0', sizeof(select_statement));
    sprintf(select_statement,"select count(*) as num from %s where business_id = '%s'  and  link_id = '%s'",table,temp3,temp4);
   
    status = OCIStmtPrepare(stmthp1, errhp,
                            select_statement, (ub4) strlen(select_statement),
                            (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);
    
    status |= OCIDefineByPos(stmthp1, &def1hp, errhp, 1, 
                             (ub1 *)&count, (sword)sizeof(count), SQLT_INT, 
                             (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) OCI_DEFAULT);
    status |= OCIStmtExecute(svchp, stmthp1, errhp, (ub4) 0, (ub4) 0,
                              NULL, NULL, (ub4) OCI_DEFAULT);
    if (status != OCI_SUCCESS)
    { 
       
    	error_proc(errhp, status);
    	printf("%s\n",oci_err); 
    	return -1;
    }
    while ((status = OCIStmtFetch(stmthp1, errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT)) != OCI_NO_DATA)
    {
    	ret = count;
      //printf("%d\n",ret);
    }
    
    /*
    --- delect
    */
    if(ret>0)
    {
     // printf("fengyxuing4\n");
      memset(delete_statement,'\0',sizeof(delete_statement));
      sprintf(delete_statement,"delete   from   %s  where   business_id = '%s'  and  link_id = '%s'",table,temp3,temp4);
      //printf("fengyxuing5\n");
      status = OCIStmtPrepare(stmthp2, errhp,
                              delete_statement, (ub4) strlen(delete_statement),
                              (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);
     
    
      status |= OCIStmtExecute(svchp, stmthp2, errhp, (ub4) 1, (ub4) 0,
                                NULL, NULL, (ub4) OCI_DEFAULT);
      if (status != OCI_SUCCESS)
      { 
      	error_proc(errhp, status);
      	printf("%s\n",oci_err); 
      	return -1;
      }
    }

   
    /*
    --- insert
    */

    memset(insert_statement, '\0', sizeof(insert_statement));
    sprintf(insert_statement, "insert into %s values(:1,:2,:3,:4)",table);

    status = OCIStmtPrepare(stmthp, errhp, 
                            insert_statement, (ub4) strlen(insert_statement),
                            (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);

    status |= OCIBindByPos(stmthp, &bnd1hp, errhp, 1,
                           (dvoid *)temp1, strlen(temp1)+1, SQLT_STR,
                           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0,
                           (ub4) 0, (ub4 *) 0, (ub4) OCI_DEFAULT);
    status |= OCIBindByPos(stmthp, &bnd2hp, errhp, 2,
                           (dvoid *)temp3, strlen(temp3)+1, SQLT_STR,
                           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0,
                           (ub4) 0, (ub4 *) 0, (ub4) OCI_DEFAULT);
    status |= OCIBindByPos(stmthp, &bnd3hp, errhp, 3,
                           (dvoid *)temp4, strlen(temp4)+1, SQLT_STR,
                           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0,
                           (ub4) 0, (ub4 *) 0, (ub4) OCI_DEFAULT);
    status |= OCIBindByPos(stmthp, &bnd4hp, errhp, 4,
                           (dvoid *)temp2, strlen(temp2)+1, SQLT_STR,
                           (dvoid *) 0, (ub2 *) 0, (ub2 *) 0,
                           (ub4) 0, (ub4 *) 0, (ub4) OCI_DEFAULT);
        
    status |= OCIStmtExecute(svchp, stmthp, errhp, (ub4) 1, (ub4) 0,
                             (CONST OCISnapshot *) 0,
                             (OCISnapshot *) 0, 
                             (ub4) OCI_DEFAULT);
   

    if (status != OCI_SUCCESS)
    { 
    	error_proc(errhp, status);
    	printf("%s\n",oci_err); 
    	return -1;
    }

    
    
    /*
    --- transaction commit
    */
    status = OCITransCommit(svchp, errhp, 0);
    
     if (status != OCI_SUCCESS)
    { 
      error_proc(errhp, status);
      printf("%s\n",oci_err); 
      return -1;
    }
     
    return 0;
}


/********************************************************************
**
**    Function    :  select_business()
**    Author      :  LanHaiBo
**
********************************************************************/
int select_business(char *table,char *configpath)
{
	  char select_statement[400];
	  char cmd[1000];
   char   date[14];       
	  char link_id[12];
   char rownum[5];
   char shell[500];
   char    businessid[7];
   char    linkid[3];   
   int   ct[10];
         


	  char full_file_name[1000];
          char data_file_xml[1000];

	  FILE *file_list;


         sword     status;
         int  filenum=0;
    	  
      numtime(ct, -1);
      memset(date,'\0',sizeof(date));
      sprintf(date,"%04d%02d%02d",ct[0],ct[1],ct[2]);

    OCIDefine *def1hp, *def2hp, *def3hp, *def4hp, *def5hp, *def6hp, *def7hp, *def8hp, *def9hp;
     
    memset(select_statement, '\0', sizeof(select_statement));
        
    sprintf(select_statement,"select link_id as id from %s order by 1",table);    
                          
    status = OCIStmtPrepare(stmthp, errhp,
                            select_statement, (ub4) strlen(select_statement),
                            (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);
                            
    
                       
    status |= OCIDefineByPos(stmthp, &def1hp, errhp, 1, 
                             (ub1 *)link_id, sizeof(link_id), SQLT_STR, 
                             (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) OCI_DEFAULT);
       

    status |= OCIStmtExecute(svchp, stmthp, errhp, (ub4) 0, (ub4) 0,
                              NULL, NULL, (ub4) OCI_DEFAULT);
 
    if (status != OCI_SUCCESS)
    {
       error_proc(errhp, status);
       memset(db_err_info, '\0', sizeof(db_err_info));
       sprintf(db_err_info, "SQL = %s\n%s", (char *)select_statement, oci_err);
       printf("%s\n", db_err_info);
       fprintf(fplog, "%s\n", db_err_info);
       fflush(fplog);
       return 0;
    }
      
       memset(shell, '\0', sizeof(shell));
	     sprintf(shell,"%s/writelog.sh",configpath);
       //printf("%s\n",shell);
     if((file_list=fopen(shell,"w")) == NULL)//打开文件 没有就?唇?
   	{
    	  	printf("can not create awst txt file!\n");
	  }
         
     fprintf(file_list,"cd %s\n", configpath);
      fprintf(file_list,"export NLS_LANG=American_America.utf8\n");
    while ((status = OCIStmtFetch(stmthp, errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT)) != OCI_NO_DATA)
    {
    	   stringtrim(link_id);
         sscanf(link_id, "%7s%3s",businessid,linkid);  
         fprintf(file_list,"write_log %s %s %s %s &\n", configpath,businessid,linkid,date); 
         	  	          	     	  	  	
    	  	  filenum++;
    	  
    }
     fprintf(file_list,"wait \n");
     fprintf(file_list,"write_bpm %s %s\n", configpath,date);
     

        	   	     	  	    
    fclose(file_list);
    

          memset(cmd, '\0', sizeof(cmd));
          sprintf(cmd, "sh  %s/writelog.sh",configpath);
          system(cmd);
    
    return 0;

   

}





