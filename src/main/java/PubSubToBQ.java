import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.transforms.JsonToRow;

public class PubSubToBQ {

    private static TupleTag<String> VALID_DATA=new TupleTag<String>(){};
    private static TupleTag<String> INVALID_DATA=new TupleTag<String>(){};
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBQ.class);


    /*
    Additional requirements 1. -> Pub/Sub topic name, Table name, topic subscription and DLQ topic are parametric.
    */
    public interface PipelineOptions extends DataflowPipelineOptions {
        @Description("Input topic name")
        String getInputTopic();
        void setInputTopic(String inputTopic);

        @Description("BigQuery table name")
        String getTableName();
        void setTableName(String tableName);

        @Description("input Subscription of PubSub")
        String getSubscription();
        void setSubscription(String subscription);

        @Description("DLQ topic of PubSub")
        String getDlqTopic();
        void setDlqTopic(String dlqTopic);
    }

    /**
     * Execution of main pipeline will be start from here.
     */
    public static void main(String[] args) {
        //To provide options at run time we willl use PipelineOptionsFactory registry methode.
        PipelineOptionsFactory.register(PipelineOptions.class);
        //with validation will check for each option.
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);
        run(options);
    }

    /**
     * Here we will apply lables to our data(VALID AND INVALID ) with the help of TupleTag.
     */
    static class JsonToCommonLog extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String json,ProcessContext processContext) throws Exception {
            // try {
            //     Gson gson = new Gson();
            //     CommonLog commonLog = gson.fromJson(json, CommonLog.class);
            //     processContext.output(VALID_DATA,commonLog);
            // }catch(Exception e){
            //     LOG.info(e.toString());
            //     processContext.output(INVALID_DATA,json);
            // }
            String[] arrJson=json.split(",");
            if(arrJson.length==3) {
                //validatios
                if(arr[0].contains("id") && arr[1].contains("name") &&arr[2].contains("surname")){
                    processContext.output(VALID_DATA,json);
                }else{
                    //Malformed data
                    processContext.output(INVALID_DATA,json);
                }
            }else{
                //Malformed data
                processContext.output(INVALID_DATA,json);
            }
        }
    }


    //building schema
    //this will help us during transforming data fom string json to Row schema type.
    public static final Schema rawSchema = Schema
            .builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();



    /**
     * This method will create pipeline and use all options provided during commandline arguments.
     */
    public static PipelineResult run(PipelineOptions options) {
        //read from subscription.
        String PubSubsubscriptionName="projects/"+options.getProject()+"/subscriptions/"+options.getSubscription();
        // Bigquery table name.
        String outputTableName=options.getProject()+":"+options.getTableName();
        //DLQ topic
        String dlqTopicName="projects/"+options.getProject()+"/topics/"+options.getDlqTopic();
        //job Name
        options.setJobName(options.getJobName());

        //create pipeline
        Pipeline pipeline = Pipeline.create(options);

        //Read data from pubsub subscription.
        PCollectionTuple rowCheck = pipeline
                .apply("ReadMessageFromPubSub", PubsubIO.readStrings()
                        .fromSubscription(PubSubsubscriptionName))

                //Filter data into two cateogory (VALID and INVALID).
                .apply("ParseJson", ParDo.of(new JsonToCommonLog()).withOutputTags(VALID_DATA, TupleTagList.of(INVALID_DATA)));

        //get PCollection<String> for both VALID and INVALID Data.
        PCollection<String> validData=rowCheck.get(VALID_DATA);
        PCollection<String> invalidData=rowCheck.get(INVALID_DATA);

        //change data from PCollection<String> to Row
        validData.apply("TransformToRow", JsonToRow.withSchema(rawSchema))
                .apply("WriteDataToTable",
                        BigQueryIO.<Row>write().to(outputTableName).useBeamSchema()
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        //write Invalid data(malformed data) to Big query.
        invalidData.apply("SendInValidDataToDLQ",PubsubIO.writeStrings().to(dlqTopicName));
        LOG.info("Building pipeline...");
        return pipeline.run();

    }
}
