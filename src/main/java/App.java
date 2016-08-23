import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import java.util.List;

/**
 * Created by adithyanayabu1 on 8/22/16.
 */
public class App {

    public static void main(String args[]) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase schoolDB = mongoClient.getDatabase("school");
        MongoCollection studentsCollection = schoolDB.getCollection("students");
        System.out.println("Students collections size: "+ studentsCollection.count());
        FindIterable studCursor = studentsCollection.find();

        studCursor.forEach((Block<Document>) student -> {
            List<Document> scoresList = (List<Document>) student.get("scores");
            Document lowestScore = null;
            for(Document score: scoresList) {
                if(lowestScore == null && "homework".equals(score.get("type"))){
                    lowestScore = score;
                    continue;
                }

                if(lowestScore != null && ((double)lowestScore.get("score") > (double)score.get("score"))) {
                    lowestScore = score;
                }
            }

            scoresList.remove(lowestScore);
            System.out.println(student.get("_id") + " : " + student.get("name")+ " the score deleted is " + lowestScore.toJson());
            studentsCollection.updateOne(Filters.eq("_id", student.get("_id")), Updates.set("scores", scoresList));

        });

        mongoClient.close();

    }
}
