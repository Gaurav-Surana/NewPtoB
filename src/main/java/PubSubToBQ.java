//import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PubSubToBQ {

    private static TupleTag<CommonLog> VALID_DATA=new TupleTag<CommonLog>(){};
    private static TupleTag<String> INVALID_DATA=new TupleTag<String>(){};
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBQ.class);


    /*
    providing custome execution  option passed by passing parameters through command lines.
     */
    public interface PipelineOptions extends DataflowPipelineOptions {

    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(PipelineOptions.class);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);
//        PipelineOptions options = (PipelineOptions) PipelineOptionsFactory.fromArgs(args).withValidation().create();

        run(options);



    }

    /**
     * A DoFn acccepting Json and outputing CommonLog with Beam Schema
     */
    static class JsonToCommonLog extends DoFn<String, CommonLog> {
        @ProcessElement
        public void processElement(@Element String json, OutputReceiver<CommonLog> r,ProcessContext processContext) throws Exception {
            try {
                Gson gson = new Gson();
                CommonLog commonLog = gson.fromJson(json, CommonLog.class);
//                r.output(commonLog);
                processContext.output(VALID_DATA,commonLog);
            }catch(Exception e){
                processContext.output(INVALID_DATA,json);
            }
        }
    }

    public static PipelineResult run(PipelineOptions options) {
        String inputTopic="projects/nttdata-c4e-bde/topics/uc1-input-topic-16";
        String subscriptionData="projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-16";
        String outputBigQueryTable="nttdata-c4e-bde:uc1_16.account";
        String dlqTopic="projects/nttdata-c4e-bde/topics/uc1-dlq-topic-16";

        //set job name
        options.setJobName(options.getJobName());

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        //read data from pubsub.
        PCollectionTuple rowCheck = pipeline
                .apply("ReadMessage", PubsubIO.readStrings()
                        .fromSubscription(subscriptionData))

                //transform
                .apply("ParseJson", ParDo.of(new JsonToCommonLog()).withOutputTags(VALID_DATA, TupleTagList.of(INVALID_DATA)));

        //Extract valid and invalid data.
        PCollection<CommonLog> validData=rowCheck.get(VALID_DATA);
        PCollection<String> invalidData=rowCheck.get(INVALID_DATA);

        //load data in BQ.
        validData.apply("WriteRawToBQ",
                BigQueryIO.<CommonLog>write().to(outputBigQueryTable).useBeamSchema()
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


        invalidData.apply(PubsubIO.writeStrings().to(dlqTopic));
        LOG.info("Building pipeline...");
        return pipeline.run();

    }
}
