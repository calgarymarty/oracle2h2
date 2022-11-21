package com.truecool.ora2h2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

public class Extractor
{
  public static void main( String[] args ) throws Exception
  {
    String url = "jdbc:oracle:thin:@//localhost:1521/orcl";
    String uid = "UID";
    String pwd = "PWD";
    String tables = "TABLE1,TABLE2";

    List<String> tableList = Arrays.asList( tables.split( "," ) );

    DdlExtractor ddlExtractor = new DdlExtractor();
    String allDdl = ddlExtractor.oracle2h2( url, uid, pwd, uid, tableList );

    File ddlFile = new File( "c:/temp/schema.sql" );

    try ( OutputStream out = new FileOutputStream( ddlFile ) )
    {
      out.write( allDdl.getBytes() );
    }

    File dmlFile = new File( "c:/temp/data.sql" );
    DmlExtractor dmlExtractor = new DmlExtractor();
    dmlExtractor.oracle2h2( url, uid, pwd, tableList, dmlFile );
  }

}
