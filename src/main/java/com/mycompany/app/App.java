package com.mycompany.app;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.LegacySQLTypeName;
//import com.google.cloud.bigquery.FieldList;



import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.*;
import com.google.api.services.bigquery.model.*;


import com.google.gson.JsonParser;


import java.util.Iterator;
import java.util.Set;
import java.util.*;
import java.util.Map.Entry;


import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;


//~ import com.google.cloud.storage.Bucket;
//~ import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
//import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;

import com.google.gson.JsonSyntaxException;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.unflattener.JsonUnflattener;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;

/**
 * Hello world!
 *
 */
public class App 
{
	
	private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
	
	private static final Logger LOG = LoggerFactory.getLogger(App.class);
	
    public static void main( String[] args ) throws FileNotFoundException
    {
		 
         System.out.println( "Start===="+LOG.isDebugEnabled() );
         
         LOG.info("Testing log message");
         LOG.error("Testing log message");
         
         Storage storage = StorageOptions.getDefaultInstance().getService();        
         
         
         String gs_location = "gs://kushal-210610/schema/myschema.json";         
         gs_location = gs_location.replace("gs://","");
         String[] arrOfStr = gs_location.split("/", 2);         
         
         String bucketName = arrOfStr[0];
		 String blobName = arrOfStr[1];
		 //~ long blobGeneration = 42;
		 //~ byte[] content = storage.readAllBytes(bucketName, blobName,
			 //~ BlobSourceOption.generationMatch(blobGeneration));
         //~ BlobId blobId = BlobId.of(bucketName, blobName);
         
         //~ byte[] content = storage.readAllBytes(blobId);
         //~ String contentString = new String(content);
         
        //System.out.println( "contentString==="+contentString);
         String currConditionsTable = "kushal-210610:demos.test";
         
         
         String PROJECT_ID = "kushal-210610";
         String DATASET_NAME ="demos";
         String TABLE_NAME ="tbl_nested";
         
         Gson gson = new Gson();
         JsonParser parser = new JsonParser();
         
         BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
         Schema schema123 = bigquery.getTable(DATASET_NAME, TABLE_NAME).getDefinition().getSchema();
         
         
         String jsonfield3 = gson.toJson(schema123);
         
         //~ JsonObject jsonobj3 =jsonfield3.getAsJsonObject();
         
         JsonObject jsonobj3 = parser.parse(jsonfield3).getAsJsonObject();
         
         String jsonfield = jsonobj3.get("fields").toString();
         System.out.println( "Schema===fields===="+jsonfield);
         
         //FieldList myfields = schema123.getFields();
         
         //System.out.println( "Schema==="+schema123.toString());
         //System.out.println( "myfields==="+gson.toJson(myfields));
         
         
         
         //String jsonfield = gson.toJson(myfields);
         
         
         
         //String path = "/tmp/myschema.json";
         
         //BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
       
         JsonArray jsonArray = gson.fromJson(jsonfield, JsonArray.class);

         //~ System.out.println(json.getClass());
         //~ System.out.println(json.toString());
         
         //InputStream is = new FileInputStream("/tmp/myschema.json");
		// String jsonTxt = IOUtils.toString(is, "UTF-8");
		 //System.out.println(jsonTxt);
         
         
         
         //String jsonfield1 = '';
		 //JsonArray jsonArray = parser.parse(jsonfield).getAsJsonArray();
         
         //~ JsonElement jsonTree1 = gson.fromJson(jsonfield, JsonElement.class);
         //~ JsonArray jobj1 = jsonTree1.getAsJsonArray();
         

		 //Set<String> keys = jsonObject.keySet();
		 
		 List<String> keys1 = getSchemaFields(jsonArray,"");
		 
		 System.out.println("Keys = " + keys1);
         
         ArrayList<String> list = new ArrayList<String>();
         Iterator<JsonElement> it = jsonArray.iterator();
			while(it.hasNext()){
				//System.out.println(it.next());				
				JsonObject jsonobj =it.next().getAsJsonObject();
				//System.out.println("=====");
				//System.out.println(jsonobj.get("subFields"));
				String el = jsonobj.get("name").toString();
				
				//System.out.println(jsonobj.get("name").getClass());
				list.add(el.replace("\"", ""));
				//list.add(el);
			}
         
         
         
         
         //System.out.println( "list==="+list);
         
         //~ ArrayList<String> al = new ArrayList<String>();
				  //~ al.add("pen");
				  //~ al.add("pencil");
				  //~ al.add("ink");
				  //~ al.add("notebook");
         
         
         //~ System.out.println( "al==="+al);
         //~ System.out.println("ArrayList contains the string 'pen': "
                                             //~ +al.contains("pen"));
        
         //~ String jsonSchema = "[{\"mode\":\"REQUIRED\",\"name\":\"identifier\",\"type\":\"STRING\"},{\"mode\":\"REQUIRED\",\"name\":\"code\",\"type\":\"STRING\"},{\"mode\":\"REQUIRED\",\"name\":\"description\",\"type\":\"STRING\"}]";


         //String testjson = "{\"name\": \"Dunlap Hubbard\", \"phone\": \"+1 (890) 543-2508\", \"gender\": \"male\", \"age\": 36, \"id\": \"59761c23b30d971669fb42ff\", \"email\": \"dunlaphubbard@cedward.com\", \"address\": \"169 Rutledge Street, Konterra, Northern Mariana Islands, 8551\", \"company\": \"CEDWARD\", \"isActive\": true,\"company1\": \"CEDWARD1\",\"company2\": \"CEDWARD2\"}";
         String testjson = "{\"id1\": \"0001\",\"type\": \"donut\",\"name\": \"Cake\",\"ppu\": 0.55,\"batters\":{\"batter\":[{ \"id\": \"1001\", \"type\": \"Regular\", \"type123\": \"Regular123\" },{ \"id\": \"1002\", \"type\": \"Chocolate\" , \"type1234\": \"Regular1234\"},{ \"id\": \"1003\", \"type\": \"Blueberry\" },{ \"id\": \"1004\", \"type\": \"Devil's Food\" }]},\"topping\":[{ \"id\": \"5001\", \"type\": \"None\" },{ \"id\": \"5002\", \"type\": \"Glazed\" },{ \"id\": \"5005\", \"type\": \"Sugar\" },{ \"id\": \"5007\", \"type\": \"Powdered Sugar\" },{ \"id\": \"5006\", \"type\": \"Chocolate with Sprinkles\" },{ \"id\": \"5003\", \"type\": \"Chocolate\" },{ \"id\": \"5004\", \"type\": \"Maple\" }],\"batters2\":{ \"id\": {\"type\": \"Chocolate\" }, \"type\": \"Devil's Food\" }}";
         
         
       
		
		//test(testjson);
		//System.out.println( "InputJSONDATA==="+testjson);
        
        //JsonObject jsonobj,tempobj;
         
        JsonObject jsonobj = parser.parse(testjson).getAsJsonObject(); 
        
        
       // String flattenedJson = JsonFlattener.flatten(jsonobj.toString());
        
        Map<String, Object> flattenedJsonMap = JsonFlattener.flattenAsMap(jsonobj.toString());
        
        
        Iterator<Entry<String, Object>> entryIt = flattenedJsonMap.entrySet().iterator();
 
		// Iterate over all the elements
		while (entryIt.hasNext()) {
			Entry<String, Object> entry = entryIt.next();
			//System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
			String newMapKey = entry.getKey().replaceAll("\\[[0-9]+\\]", ""); 
			if (!keys1.contains(newMapKey)) {
				//System.out.println("Deleted=="+entry.getKey()+ " Value : " + entry.getValue());
				entryIt.remove();
			 }
			// Check if Value associated with Key is 10
			
		}
        
        
        //Map<String, Object> flattenedJsonMapTemp = flattenedJsonMap;
        
        //~ for (Map.Entry<String, Object> entry : flattenedJsonMap.entrySet()) {
            //~ System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
            //~ //flattenedJsonMapTemp.remove("batters2.type");
        //~ }
        
        //System.out.println( "flattenedJsonMap==="+flattenedJsonMap);
        
        //flattenedJsonMap.forEach((k, v) -> System.out.println(k + " : " + v));
        
        //flattenedJsonMap.remove("batters.batter[0].type123");
        //flattenedJsonMap.remove("batters2.type");
        
        //System.out.println( "flattenedJsonMap===Removed=="+flattenedJsonMap);
        
        String flattenedJson = gson.toJson(flattenedJsonMap);
        
        String nestedJson = JsonUnflattener.unflatten(flattenedJson);
        
        
        
        //System.out.println( "nestedJson==="+nestedJson);
        //~ HashMap<String,Object> result =
        //~ new ObjectMapper().readValue(jsonobj, HashMap.class);
        
        
        
        JsonElement jsonobj_ele = parser.parse(testjson); 
        
       // System.out.println( "jsonobj==="+jsonobj.get("batters"));
        
        JsonObject tempobj = parser.parse(testjson).getAsJsonObject(); 
        
        //JsonObject tempobj12 = parseJSON(jsonobj,tempobj,keys1,"");
        
        
        //System.out.println( "tempobj12======"+tempobj12);
		
		Set<String> keys = jsonobj.keySet();
		
		
		System.out.println( "jsonobj==="+keys);
		System.out.println( "jsonobj==="+keys.contains("ppu1"));
		//~ for (Entry<String, JsonElement> valueEntry : jsonobj.entrySet()) {
             //~ JsonElement element = valueEntry.getValue();
             //~ if (element.isJsonPrimitive()) {
				  //~ System.out.println( "element==="+element);
				  //~ if (!keys1.contains(valueEntry.getKey())) {
					  //~ System.out.println( "Element not found :-"+valueEntry.getKey()); 
					  
				   //~ } 
				 
			  //~ }else if (element.isJsonObject()){
				  //~ System.out.println( "isJsonObject==="+element);				  
			  //~ }else if (element.isJsonArray()){
				  //~ System.out.println( "isJsonArray==="+element);				  
			  //~ }
              
              //~ //System.out.println( "valueEntry.getKey()==="+valueEntry.getKey());
              //~ //System.out.println( "element==="+element.getClass());
    
       //~ }
		
		
		//System.out.println( "jsonobj==="+keys);
		ArrayList<String> extraFields = new ArrayList<String>();
		
		List<TableRow> rawFields = new ArrayList<>();
		
		for (String s : keys) {
			//System.out.println(s);
			//System.out.println("Element not found value :- "+jsonobj.get(s).getClass());
			if (!list.contains(s)) {
				//System.out.println("Element not found :- "+s);
				//System.out.println("Element not found value :- "+jsonobj.get(s));
				rawFields.add(new TableRow().set("name",s).set("value", jsonobj.get(s)));
				//extraFields.add(s);
				tempobj.remove(s);
			}
			
		}
		//String extra = extraFields.toString();
		//System.out.println( "extraFields==="+rawFields);
		//System.out.println( "jsonobj==removed="+tempobj);
		//System.out.println( "jsonobj==="+jsonobj);
		
		//keys.forEach(System.out::println);
		// Json schema uses "fields"
		// com.google.cloud.bigquery.Field uses "subFields"
		// FIXME Unable to use @SerializedName policy
		//~ jsonSchema = jsonSchema.replace("\"fields\"", "\"subFields\"");
		
		
		//~ JsonElement jsonTree = gson.fromJson(testjson, JsonElement.class);
        //~ JsonObject jobj = jsonTree.getAsJsonObject();
        //~ String name = jobj.get("name").getAsString();
        
        
        
        //~ String test = "{anotherField=testing field added, name=Test User12, phone=+1 (890) 543-2508, gender=male, age=63, id=59761c23b30d971669fb42ff, email=test@test.com, address=169 Rutledge Street, Konterra, Northern Mariana Islands, 8551, company=CEDWARD1, isActive=true}";
        
        //~ String jsonEl = gson.toJson(test);
        //System.out.println(jsonEl);
        //System.out.println(jsonTree.isJsonObject());
       // System.out.println(name);
        //System.out.println(jelem.mode);
		// Deserialize schema with custom Gson
		//Field[] fields = getGson().fromJson(jsonSchema, Field[].class);
		//Schema schema = Schema.of(fields);
		
		
		//~ List<TableFieldSchema> fields1 = new ArrayList<>();
   
		//~ fields1.add(new TableFieldSchema().setName("message").setType("STRING"));
		//~ fields1.add(new TableFieldSchema().setName("name").setType("STRING"));
		//~ TableSchema schema1 = new TableSchema().setFields(fields1);

       
		//System.out.println(schema.toString());
		//System.out.println(schema1.toString());
        
        //~ StandardTableDefinition tableDefinition = StandardTableDefinition.of(schema);
        //~ Table createdTable = bigquery.create(TableInfo.of(currConditionsTable, tableDefinition));
        //~ BigQueryIO.writeTableRows().to(currConditionsTable)//
            //~ .withSchema(schema1)//
            //~ .withoutValidation()
            //~ .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            //~ .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER);
        
        //~ System.out.println( "End program" );
            
      
      //String payloadString = gson.toJson(customRow);  
      //System.out.println(customRow.getClass()); 
      //System.out.println(TableRow.size()); 
      
    //  String test1 = "topping[4].type";
      
     // String test2 = test1.replaceAll("\\[[0-9]+\\]", "");
     // System.out.println("===="+test2);
     // String time1 ="2018-09-10 09:26:55.949 UTC";
      //String messageTimestamp = DATE_FORMATTER.print(time1.toDateTime(DateTimeZone.UTC));
      
      String nested_field = "attr.key.name";
      
      String[] arrOfStr1 = nested_field.split("\\.",2); 
      
      System.out.println("arrOfStr==="+arrOfStr1.length);
      System.out.println("arrOfStr==="+arrOfStr1[0]);
      System.out.println("arrOfStr==="+arrOfStr1[1]);
      
      TableRow tabelRow = new TableRow();
      
      tabelRow.set( "name", "test" );
      tabelRow.set( "name1", "test1" );
      
      List<TableRow> attributeTableRows = Lists.newArrayList();
      
      attributeTableRows.add(new TableRow().set("key", "keyvalue").set("value", "value123"));
      
      tabelRow.set( "attribute", attributeTableRows);
      //System.out.println("tabelRow"+tabelRow.toString());
      
      
      String my_test ="en-au	2661327960	581	375	32			sport		0		0	0		JS-2.5.0	1	2	Y	304		2	1.000000000000	AUD	0	bd79c3b2e4845a245f6f5f93a54f2eb3	0	2018-10-04 12:32:26	telstra.net		0		news corp au	foxs|sport|story|‘staggering and disappointing’: legend slams molan criticism	bd79c3b2e4845a245f6f5f93a54f2eb3	not set		anonymous	not set	af68975ab03bd15f7d3f7bc965354822	‘staggering and disappointing’: legend slams molan criticism		news.com.au	fox sports	2018-10-04 08:43:00		12:32 PM|Thursday		Repeat						fox sports web					https://www.foxsports.com.au/nrl/liz-ellis-hits-out-at-critics-of-footy-show-host-erin-molan/news-story/af68975ab03bd15f7d3f7bc965354822						sport							not logged in				nrl	2018-10-04 08:43:55		375x667|ios|12.0.0	1.0-wpcom+theme_fox_sports_australia_articlepage								94					true:false:false:true:true:true													comments+story	200,207,201,262=94,264,100,101,102,103,104,108,109,110,111,113,114,115,116,118,119,121,123,133,145,149,151,152,159,164,175,176,179,189,10009	0	https://www.foxsports.com.au/boxing/scott-westgarth-dies-after-being-rushed-to-hospital-after-winning-fight/news-story/848977172a1d4730b805aa2634933eb0	foxs|sport|story|boxer dies at age 31 after winning fight	http://m.facebook.com	1519718217	hamilton	aus	36106	nsw	2303	news corp au|fox sports|fox sports web|sport|nrl					1	1538620346	3304162033601871872	5466139326217715724	U	0	101.187.64.182		1.6	N	7	36	1538620314	0	0	29327662672450744921969645871410994513	7180628																		0		nrl			0	4117375406		0					https://www.foxsports.com.au/nrl/liz-ellis-hits-out-at-critics-of-footy-show-host-erin-molan/news-story/af68975ab03bd15f7d3f7bc965354822	foxs|sport|story|‘staggering and disappointing’: legend slams molan criticism	0		Y				581	375		sport	Y	AUD	1538620346	bd79c3b2e4845a245f6f5f93a54f2eb3	news corp au	foxs|sport|story|‘staggering and disappointing’: legend slams molan criticism	bd79c3b2e4845a245f6f5f93a54f2eb3	not set		anonymous	not set	af68975ab03bd15f7d3f7bc965354822	‘staggering and disappointing’: legend slams molan criticism		news.com.au	fox sports	2018-10-04 08:43:00		12:32 PM|Thursday		Repeat						fox sports web					https://www.foxsports.com.au/nrl/liz-ellis-hits-out-at-critics-of-footy-show-host-erin-molan/news-story/af68975ab03bd15f7d3f7bc965354822						sport							not logged in				nrl	2018-10-04 08:43:55		375x667|ios|12.0.0	1.0-wpcom+theme_fox_sports_australia_articlepage			http://m.facebook.com	Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/16A366 [FBAN/FBIOS;FBAV/191.0.0.42.96;FBBV/125515388;FBDV/iPhone7,2;FBMD/iPhone;FBSN/iOS;FBSV/12.0;FBSS/2;FBCR/Telstra;FBID/phone;FBLC/en_GB;FBOP/5;FBRV		Facebook App (iOS)		94					true:false:false:true:true:true											101.187.64.182		comments+story	200,207,201,262=94.00,264,100,101,102,103,104,108,109,110,111,113,114,115,116,118,119,121,123,133,145,149,151,152,159,164,175,176,179,189,10009,158,155,156,174	news corp au|fox sports|fox sports web|sport|nrl					N																				nrl		0					https://www.foxsports.com.au/nrl/liz-ellis-hits-out-at-critics-of-footy-show-host-erin-molan/news-story/af68975ab03bd15f7d3f7bc965354822	foxs|sport|story|‘staggering and disappointing’: legend slams molan criticism	foxs|sport|story|‘staggering and disappointing’: legend slams molan criticism		Y			;;;;;1215=::hash::0	news corp au	https://www.foxsports.com.au/nrl/liz-ellis-hits-out-at-critics-of-footy-show-host-erin-molan/news-st	bd79c3b2e4845a245f6f5f93a54f2eb3	not set		anonymous	not set	af68975ab03bd15f7d3f7bc965354822	‘staggering and disappointing’: legend slams molan criticism		news.com.au	fox sports	2018-10-04 08:43:00		12:32 PM|Thursday		Repeat						fox sports web	Less than 1 day	foxs|sport|story|watch: cctv shows ronaldo dancing with rape accuser as police re-open case	39|31					news corp au|fox sports|fox sports web|sport|nrl			sport						portrait	not logged in				nrl	2018-10-04 08:43:55			1.0-wpcom+theme_fox_sports_australia_articlepage			http://m.facebook.com	Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobi		Facebook App (iOS)		94					true:false:false:true:true:true											bd79c3b2e4845a245f6f5f93a54f2eb3-af68975ab03bd15f7d3f7bc965354822-1538620345945-972508		comments+story		http://m.facebook.com	0													4/9/2018 12:31:53 4 -600														3373411943	565725828	0	::hash::0	1976685907			news corp au	https://www.foxsports.com.au/nrl/liz-ellis-hits-out-at-critics-of-footy-show-host-erin-molan/news-st	bd79c3b2e4845a245f6f5f93a54f2eb3	not set		anonymous	not set	af68975ab03bd15f7d3f7bc965354822	‘staggering and disappointing’: legend slams molan criticism		news.com.au	fox sports	2018-10-04 08:43:00		12:32 PM|Thursday		Repeat						fox sports web	Less than 1 day	foxs|sport|story|watch: cctv shows ronaldo dancing with rape accuser as police re-open case	39|31								sport						portrait	not logged in				nrl	2018-10-04 08:43:55			1.0-wpcom+theme_fox_sports_australia_articlepage								94					true:false:false:true:true:true											bd79c3b2e4845a245f6f5f93a54f2eb3-af68975ab03bd15f7d3f7bc965354822-1538620345945-972508		comments+story		0	facebook.com	9	http://m.facebook.com	192	375x667	Y	0	0	1	ss											0		www13.syd2.omniture.com	4/9/2018 12:32:26 4 -600					N				Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/16A366 [FBAN/FBIOS;FBAV/191.0.0.42.96;FBBV/125515388;FBDV/iPhone7,2;FBMD/iPhone;FBSN/iOS;FBSV/12.0;FBSS/2;FBCR/Telstra;FBID/phone;FBLC/en_GB;FBOP/5;FBRV/126377540]	212956657		100012035	newscorpau-global	m.facebook.com	6	m.facebook.com	6	0	0											0	0	N	0	5		26	2	http://m.facebook.com	0	https://www.foxsports.com.au/football/video-of-cristiano-ronaldo-dancing-with-rape-accuser-kathryn-mayorga-in-las-vegas-nightclub-hours-before-alleged-attack/news-story/bbf8aab9193a5e60547f2c7c34bb10c3	foxs|sport|story|watch: cctv shows ronaldo dancing with rape accuser as police re-open case	1538620314	0	0												2018-02-27 18:56:54																																																																																																																																													no plugins	29327662672450744921969645871410994513			bd79c3b2e4845a245f6f5f93a54f2eb3-af68975ab03bd15f7d3f7bc965354822-1538620345945-972508										161de8b1977-2221f9e4-7ad3-4879-86f0-37d88a495f47																				2018-02-27 18:56:54																																																																																																																																													no plugins	29327662672450744921969645871410994513			bd79c3b2e4845a245f6f5f93a54f2eb3-af68975ab03bd15f7d3f7bc965354822-1538620345945-972508										161de8b1977-2221f9e4-7ad3-4879-86f0-37d88a495f47									";
      
      System.out.println("tabelRow"+my_test.length());
      
      String[] words2 = my_test.split( "\t",-1);
	  System.out.println("Original Message:   --  " +words2.length);
	  String field_val = "f";
	  field_val = field_val.toLowerCase();
	  
	  if( field_val.equals("0") || field_val.equals("1") )
	   {	   
		   field_val = field_val.equals("0")?"false":"true";										       
		   //tabelRow.set( fieldsMapss.get(s),  field_val );
	   }else if(field_val.equals("t") || field_val.equals("f")){
		   
		  field_val = field_val.equals("f")?"false":"true";										       
		  //tabelRow.set( fieldsMapss.get(s),  field_val ); 											   
	   }
	  
	  
	  System.out.println(field_val);
      
    }
    
    
    //~ public static Gson getGson() {
		//~ JsonDeserializer<LegacySQLTypeName> typeDeserializer = (jsonElement, type, deserializationContext) -> {
			//~ return LegacySQLTypeName.valueOf(jsonElement.getAsString());
		//~ };

		//~ JsonDeserializer<FieldList> subFieldsDeserializer = (jsonElement, type, deserializationContext) -> {
			//~ Field[] fields = deserializationContext.deserialize(jsonElement.getAsJsonArray(), Field[].class);
			//~ return FieldList.of(fields);
		//~ };

		//~ return new GsonBuilder()
			//~ .registerTypeAdapter(LegacySQLTypeName.class, typeDeserializer)
			//~ .registerTypeAdapter(FieldList.class, subFieldsDeserializer)
			//~ .create();
	//~ }
	
  
  
  public static List<String> getSchemaFields(final JsonArray jsonFields,final String prefix){
	  List<String> fields = new ArrayList<>();	  
	  for(int i=0; i < jsonFields.size(); i++){		  
		 JsonObject jsonobj = jsonFields.get(i).getAsJsonObject();
		 JsonObject subfieldJson = jsonobj.get("type").getAsJsonObject();		 
		 String newPrefix = prefix + jsonobj.get("name").toString();
		 if (subfieldJson.get("fields") ==null){
			fields.add(newPrefix.replace("\"", ""));
		  }else{						
			JsonArray subfield = subfieldJson.get("fields").getAsJsonArray();
			fields.addAll(getSchemaFields(subfield,newPrefix+ "."));
		   
		  }
	  }
	
	  return fields;
  }
  
  
  public static JsonObject parseJSON(final JsonObject json, JsonObject tempobj, List<String> schemaFields, String prefix) {
	  
	  for (Entry<String, JsonElement> valueEntry : json.entrySet()) {
             JsonElement element = valueEntry.getValue();
             
              if (element.isJsonPrimitive()) {
				  String newPrefix = prefix + valueEntry.getKey();
				  //System.out.println( newPrefix+"====element==="+element);
				  if (!schemaFields.contains(newPrefix)) {					  
					  System.out.println( "Element not found :-"+newPrefix); 
					  //System.out.println( "tempobj==="+tempobj);
					  //tempobj.remove(key);					  
				   }
				 
			  }else if (element.isJsonObject()){
				  System.out.println( "isJsonObject==="+element);
				  String newPrefix = prefix + valueEntry.getKey();
				  System.out.println( "newPrefix==="+newPrefix);
				  tempobj = parseJSON(element.getAsJsonObject(),element.getAsJsonObject(),schemaFields,newPrefix+ ".");				  
			  }else if (element.isJsonArray()){
				  System.out.println( "isJsonArray==="+element);
				  String newPrefix = prefix + valueEntry.getKey();
				  JsonArray el  = element.getAsJsonArray();
				  for(int i=0; i < el.size(); i++){
					  JsonObject jsonobj = el.get(i).getAsJsonObject();
					  tempobj = parseJSON(jsonobj,jsonobj,schemaFields,newPrefix+ ".");
				  }
				  				  
			  }
           
    
       }
	  
	  
	 return tempobj; 
	  
  }
  
  //~ public static List<String> operate(final JsonElement jsonElement, final String prefix, final boolean firstLayer) {
    //~ if(jsonElement.isJsonObject() || (!firstLayer && jsonElement.isJsonArray())) {
        //~ List<String> keys = new ArrayList<>();
        //~ if(jsonElement.isJsonObject()) {
            //~ JsonObject jObj = jsonElement.getAsJsonObject();
            //~ for(Map.Entry<String, JsonElement> entry : jObj.entrySet()) {
                //~ JsonElement value = entry.getValue();
                //~ String newPrefix = prefix + entry.getKey();
                //~ if(value.isJsonArray() || value.isJsonObject()) {
                    //~ keys.add(newPrefix);
                    //~ keys.addAll(operate(value, newPrefix + "_", false));
                //~ }
            //~ }
        //~ } else {
            //~ JsonArray array = jsonElement.getAsJsonArray();
            //~ for(JsonElement element : array) {
                //~ keys.addAll(operate(element, prefix, false));
            //~ }
        //~ }
        //~ return keys;
    //~ } else {
        //~ return Collections.emptyList();
    //~ }
//~ }


//~ public static void test(String json) {
    //~ JsonElement jsonElement = new JsonParser().parse(json);
    //~ List<String> keys = operate(jsonElement, "", true);
    //~ System.out.println("Keys = " + keys);
//~ }
  	
    
}
