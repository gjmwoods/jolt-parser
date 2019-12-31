package com.example.joltparser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.internal.value.BytesValue;
import org.neo4j.driver.internal.value.DateValue;
import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.PointValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.Point;

public class JoltParserApplication
{

    final static String QUERY = "{\n" +
                                "    \"statements\": [\n" +
                                "        {\n" +
                                "            \"statement\": \"CREATE (n:MyLabel:AnotherLabel {aString: 'a string', aDate: date('2015-07-21'), anotherString: 'another string',aNumber: 1234,arrayOfStrings: ['s1', 's2'], arrayOfDates: [date('2015-07-29'), date('2015-07-30')]})RETURN n as N, n.aString, n.aDate, n.aNumber, [n, n.aNumber, n.aDate] as A\",\n" +
                                "            \"resultDataContents\": [\"jolt\"]\n" +
                                "        }\n" +
                                "    ]\n" +
                                "}";

    final static String QUERY_FALSE = "{\n" +
                                      "    \"statements\": [\n" +
                                      "        {\n" +
                                      "            \"statement\": \"UNWIND range(1,1,1) as n RETURN false\",\n" +
                                      "            \"resultDataContents\": [\"jolt\"]\n" +
                                      "        }\n" +
                                      "    ]\n" +
                                      "}";

    final static String QUERY_FALSE_NULL_STRING = "{\n" +
                                                  "    \"statements\": [\n" +
                                                  "        {\n" +
                                                  "            \"statement\": \"UNWIND range(1,1,1) as n RETURN false, null, 'String'\",\n" +
                                                  "            \"resultDataContents\": [\"jolt\"]\n" +
                                                  "        }\n" +
                                                  "    ]\n" +
                                                  "}";

    final static String QUERY_NODE_SIMPLE = "{\n" +
                                            "    \"statements\": [\n" +
                                            "        {\n" +
                                            "            \"statement\": \"CREATE (n:MyLabel {aString: 'a string', aDate: date('2015-07-21'), anotherString: 'another string',aNumber: 1234,arrayOfStrings: ['s1', 's2'], arrayOfDates: [date('2015-07-29'), date('2015-07-30')], aFloat: 3.7, aPoint: point({ latitude:toFloat('13.43'), longitude:toFloat('56.21')})})RETURN n as N\",\n" +
                                            "            \"resultDataContents\": [\"jolt\"]\n" +
                                            "        }\n" +
                                            "    ]\n" +
                                            "}";
    final static String QUERY_REL = "{\n" +
                                    "    \"statements\": [\n" +
                                    "        {\n" +
                                    "            \"statement\": \"CREATE (a)-[r:RELTYPE { levels: [1,2], age: 12.4, city: 'London', licence: true, date: date('2015-07-30'), location: point({ latitude:toFloat('13.43'), longitude:toFloat('56.21')})}]->(b) RETURN r\",\n" +
                                    "            \"resultDataContents\": [\"jolt\"]\n" +
                                    "        }\n" +
                                    "    ]\n" +
                                    "}";

    final static String QUERY_PATH = "{\n" +
                                     "    \"statements\": [\n" +
                                     "        {\n" +
                                     "            \"statement\": \"MATCH (martin:Person { name: 'Martin Sheen' }),(oliver:Person { name: 'Oliver Stone' }), p = shortestPath((martin)-[*..15]-(oliver)) RETURN p\",\n" +
                                     "            \"resultDataContents\": [\"jolt\"]\n" +
                                     "        }\n" +
                                     "    ]\n" +
                                     "}";

    public static void main( String[] args ) throws IOException
    {
        JsonNode neoNode = getResultJson(QUERY);

        List<Record> records = parse( neoNode );

        System.out.println( records.toString() );
    }

    private static List<Record> parse( JsonNode jsonNode ) throws IOException
    {
        List<Record> records = new ArrayList<>();

        JsonNode resultsNode = extractJolt( jsonNode );

        ArrayNode fields = (ArrayNode) resultsNode;
        Value[] values = new Value[fields.size()];

        for ( int i = 0; i < fields.size(); i++ )
        {
            values[i] = parseValue( fields.get( i ) );
        }

        Record record = new InternalRecord( extractColumns( jsonNode ), values );

        records.add( record );

        return records;
    }

    public static Value parseValue( JsonNode valueNode ) throws IOException
    {
        if ( valueNode.getNodeType() == JsonNodeType.OBJECT )
        {
            return parseComplex( valueNode );
        }
        else if ( valueNode.getNodeType() == JsonNodeType.ARRAY )
        {
            ArrayNode arrayNode = (ArrayNode) valueNode;

            Value[] valueArray = new Value[arrayNode.size()];

            for ( int i = 0; i < arrayNode.size(); i++ )
            {
                valueArray[i] = parseValue( arrayNode.get( i ) );
            }

            return new ListValue( valueArray );
        }
        else
        {
            return parsePrimitive( valueNode );
        }
    }

    public static Value parseComplex( JsonNode valueNode ) throws IOException
    {
        for ( Iterator<Map.Entry<String,JsonNode>> it = valueNode.fields(); it.hasNext(); )
        {
            Map.Entry<String,JsonNode> currentNode = it.next();
            String field = currentNode.getKey();

            if ( isNode( field ) )
            {
                String[] nodeSplit = splitNode( field );

                List<String> labels = new ArrayList<>( Arrays.asList( nodeSplit ).subList( 1, nodeSplit.length ) );

                Map<String,Value> properties = parseProperties( currentNode.getValue() );

                return new InternalNode( Long.parseLong( nodeSplit[0] ), labels, properties ).asValue();
            }
            else if ( isRelationship( field ) )
            {
                String[] relRaw = field.split( "-" );
                String[] startNode = splitNode( relRaw[0] );
                String[] endNode = splitNode( relRaw[2].substring( 1 ) );

                String[] relationship = splitRelationship( relRaw[1] );

                Map<String,Value> properties = parseProperties( currentNode.getValue() );

                return new InternalRelationship( Long.parseLong( relationship[0] ),
                                                 Long.parseLong( startNode[0] ),
                                                 Long.parseLong( endNode[0] ),
                                                 relationship.length > 1 ? relationship[1] : null,
                                                 properties ).asValue();
            }
            else if ( field.equals( ".." ) )
            {
                List<Entity> nodeRelAlternating = new ArrayList<>();

                ArrayNode nodeRelNodeArray = (ArrayNode) currentNode.getValue();

                nodeRelNodeArray.forEach( jsonNode ->
                                          {
                                              try
                                              {
                                                  nodeRelAlternating.add( parseValue( jsonNode ).asEntity() );
                                              }
                                              catch ( IOException e )
                                              {
                                                  e.printStackTrace();
                                              }
                                          } );

                InternalPath path = new InternalPath( nodeRelAlternating );
                return new PathValue( path ).asValue();
            }
            else if ( field.equals( "Z" ) )
            {
                return new IntegerValue( currentNode.getValue().asLong() );
            }
            else if ( field.equals( "R" ) )
            {
                return new FloatValue( currentNode.getValue().asDouble() );
            }
            else if ( field.equals( "#" ) )
            {
                return new BytesValue( currentNode.getValue().binaryValue() );
            }
            else if ( field.equals( "T" ) )
            {
                return new DateValue( LocalDate.parse( currentNode.getValue().asText() ) ); //todo other date types
            }
            else if ( field.equals( "@" ) )
            {
                return new PointValue( parsePoint( currentNode.getValue().asText() ) );
            }
        }

        return new IntegerValue( 11 ).asValue(); //todo safety net
    }

    private static Point parsePoint( String rawPoint ) //todo 3d points + other srid
    {
        String[] split = rawPoint.split( " " );
        return new InternalPoint2D( 4326, Double.parseDouble( split[1].substring( 1 ) ), Double.parseDouble( split[2].substring( 0, split.length - 1 ) ) );
    }

    private static Map<String,Value> parseProperties( JsonNode propertyValues ) throws IOException
    {
        Map<String,Value> properties = new HashMap<>();

        propertyValues.fields().forEachRemaining( fieldName ->
                                                  {
                                                      try
                                                      {
                                                          properties.put( fieldName.getKey(), parseValue( fieldName.getValue() ) );
                                                      }
                                                      catch ( IOException e )
                                                      {
                                                          e.printStackTrace();
                                                      }
                                                  } );
        return properties;
    }

    public static Value parsePrimitive( JsonNode valueNode )
    {
        if ( valueNode.isBoolean() )
        {
            return BooleanValue.fromBoolean( valueNode.booleanValue() );
        }
        else if ( valueNode.isNull() )
        {
            return NullValue.NULL;
        }
        else
        {
            {
                return new StringValue( valueNode.textValue() );
            }
        }
    }

    private static JsonNode getResultJson(String query) throws IOException
    {
        RestTemplate restTemplate = new RestTemplate();

        String fooResourceUrl
                = "http://localhost:7474/db/neo4j/tx/commit";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType( MediaType.APPLICATION_JSON );

        HttpEntity<String> entity = new HttpEntity<>( query, headers );

        ResponseEntity<String> response = restTemplate.exchange( fooResourceUrl, HttpMethod.POST, entity, String.class );

        ObjectMapper om = new ObjectMapper();

        JsonNode jsonNode = om.readTree( response.getBody() );

        return jsonNode.get( "results" ).get( 0 );
    }

    private static JsonNode extractJolt( JsonNode jsonNode )
    {
        return jsonNode.get( "data" ).get( 0 ).get( "jolt" );
    }

    private static List<String> extractColumns( JsonNode jsonNode )
    {
        ArrayNode columns = (ArrayNode) jsonNode.get( "columns" );

        List<String> columnList = new ArrayList<>();

        for (int i = 0; i<columns.size(); i++)
        {
            columnList.add( columns.get( i ).textValue() );
        }

        return columnList;
    }

    private static boolean isRelationship( String field )
    {
        Pattern pattern = Pattern.compile( "\\([0-9]+(:[a-zA-Z]+)*\\)-\\[[0-9]+(:[a-zA-Z]+)*\\]->\\([0-9]+(:[a-zA-Z]+)*\\)" );

        return pattern.matcher( field ).find();
    }

    private static boolean isNode( String field )
    {
        Pattern pattern = Pattern.compile( "\\([0-9]+(:[a-zA-Z]+)*\\)" );

        return pattern.matcher( field ).matches();
    }

    private static String[] splitNode( String node )
    {
        String nodeRaw = node.substring( 1, node.length() - 1 );
        return nodeRaw.split( ":" );
    }

    private static String[] splitRelationship( String rel )
    {
        String relRaw = rel.substring( 1, rel.length() - 1 );
        return relRaw.split( ":" );
    }
}
