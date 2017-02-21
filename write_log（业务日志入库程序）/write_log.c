#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ocilib.h"
#include "public.h"
#include "public.c"

#define LINE  30000
#define COLUMN_NUM 7    //字段个数


/*
----- 主函数参数描述：argc=2 argv[1]：cr01数据文件名
*/
int main(int argc,char *argv[])
{
    char user[30], passwd[30], dbname[30];
    char  configpath[100],logpath[100],table1[500],table2[500],table3[500];
    char buf[LINE];
    char temp[COLUMN_NUM][2000]; 
    char date[14],utctime[20],time[25];
    int   ct[10],type=1;
    int  ret1,ret2,row=0;
    char filename[500],file[100],config[500];
	char filename_path[100];
	char file_path[50];

    FILE   *fp;
    FILE   *fp1;
    int  i,j,ret,num=0,record=0;
    FILE *hua_fp;
    FILE *fp_config;
    
    /*
    ----- variable initialization
    */
    numtime(ct, -1);

    memset(temp,'\0',sizeof(temp));
    memset(buf,'\0',sizeof(buf));
    
    memset(user, '\0', sizeof(user));
    memset(passwd, '\0', sizeof(passwd));
    memset(dbname, '\0', sizeof(dbname));
     memset(configpath, '\0', sizeof(configpath));
    memset(logpath, '\0', sizeof(logpath));
    memset(table1, '\0', sizeof(table1));
     memset(table2, '\0', sizeof(table2));
    memset(table3, '\0', sizeof(table3));
   

   /* 
    -----1. 读配置文件 
    */
      memset(config, '\0', sizeof(config));
	     sprintf(config,"%s/config.txt",argv[1]);
    

   fp_config = fopen(config,"r");
        
    if (fp_config == NULL)
    { 
    	printf("open file error\n");  
     fclose(fp_config); 
     exit(-1);   
    }
    else
  {
 
  while(fgets(buf,LINE,fp_config))
    {
    	
    	num=0;
    	num=stringsplit(buf,':',2,temp);  	
		  
   
    	if(num==2)
    	{
       if (record==0)  
      {   
         
           sprintf(configpath,"%s",temp[1]);
                 } 
      else  if(record==1)
      {   sprintf(logpath,"%s",temp[1]);
                 } 
      else  if(record==2)
      {   sprintf(user,"%s",temp[1]);
         // printf("%s\n",user);
      } 
      else  if(record==3)
      {   sprintf(passwd,"%s",temp[1]);
         // printf("%s\n",passwd);

      } 
      else  if(record==4)
      {   sprintf(dbname,"%s",temp[1]);
         // printf("%s\n",dbname);
     } 
      else  if(record==5)
      {   sprintf(table1,"%s",temp[1]);} 
      else  if(record==6)
      {   sprintf(table2,"%s",temp[1]);} 
      else  if(record==7)
      {   sprintf(table3,"%s",temp[1]);} 

    		
    		record++;
    		
    	}

    	   memset(buf,'\0',sizeof(buf));         
        memset(temp,'\0',sizeof(temp));
     }
       fclose(fp_config);	
 }


   /* 
    -----2. 连接数据库
    */
   
    ret = logon(user, passwd, dbname);
   

    if (ret != 0)
    {
    	printf("Connect Oracle Faillog ...\n");
    	printf("%s\n", oci_err);
    	logoff();
    	exit(-1);
    }

  /* 
    -----3. read 偏移量 
    */
      memset(filename, '\0', sizeof(filename));
	     sprintf(filename,"%s/fseek/%s_%s_%s.txt",logpath,argv[2],argv[3],argv[4]);

      memset(file, '\0', sizeof(file));
	     sprintf(file,"%s/%s/%s_%s_%s.log",logpath,argv[2],argv[2],argv[3],argv[4]);


   fp1 = fopen(filename,"r");
        
    if (fp1 == NULL)
    { 
    	printf("open file error %s\n",filename);     
    }
    else
  {

  while(fgets(buf,LINE,fp1))
    {
    	
    	/*
    	--- process a line
    	*/
    	num=0;
    	num=stringsplit(buf,' ',1,temp);  	
		
    	if(num==1)
    	{                
       row=atoi(temp[0]);
       //printf("%d\n",row);
    	}
     } 
    fclose(fp1);	
 }
    
		/*
		-----4. 日志处理
		*/
	
	fp = fopen(file,"r");
        
    if (fp == NULL)
    { 
    	printf("open file error: %s\n",file);
      exit(-1);
    }
 
    fseek(fp, row*1L,SEEK_SET);
      
         while(fgets(buf,LINE,fp))
    {
  
    	num=0;
    //	printf("%s\n",buf);
    	num=stringsplit(buf,'--',COLUMN_NUM,temp);  			
      // printf("%d\n",num);
			//printf("%s %s %s %s %s %s\n",temp[0],temp[1],temp[2],temp[3],temp[4],temp[5]);
    	if(num==6)
    	{
       if ((strcmp(temp[1],"INFO")==0) || (strcmp(temp[1],"ERROR")==0))  
      {                
    		ret1=insert_detail_log(table2,temp[0],temp[1],temp[2],temp[3],temp[4],temp[5]);
       	if(ret1!=0)
    		{
    			printf("call insert_collect_log() failed.\n");
    			//exit(-1);
    		}
      ret2=insert_collect_log(table3,temp[0],temp[1],temp[2],temp[3]);
        if(ret2!=0)
    		{
    			printf("call insert_detail_log() failed.\n");
    			//exit(-1);
    		}

       } 
     else
           {
    			printf("no right  log data.\n");
    	    	}
           		
    		record++;
    		
    	}

    	   memset(buf,'\0',sizeof(buf));         
        memset(temp,'\0',sizeof(temp));
    } 
    printf("processed records: %d\n",record);
    row=ftell(fp); 	
      

	   /*
		-----5. 写入新的日志偏移量
		*/

	     numtime(ct, -1);
      memset(time,'\0',sizeof(time));
      sprintf(time,"%02d%02d",ct[3],ct[4]);

	if((hua_fp=fopen(filename,"w"))==NULL)//打开文件 没有就创建
	{
		printf("can not create awst txt file!\n");
	}
  
     fprintf(hua_fp,"%d\n",row);
    
 
      fclose(fp); 
      fclose(hua_fp);  
       logoff();    
    exit(0);
}

