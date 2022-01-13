import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * Creating schema for storing messages receive from Pubsub topic.
 */
@DefaultSchema(JavaFieldSchema.class)
    public class CommonLog {
        int id;
        String name;
        String surname;
    }



