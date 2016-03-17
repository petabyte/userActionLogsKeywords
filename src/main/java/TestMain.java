import org.apache.hadoop.io.Text;

import java.io.*;

/**
 * Created by gsanchez on 2/25/16.
 */
public class TestMain {
    public static void main(String args[]) throws IOException {
        KeywordParser keywordParser = new KeywordParser();


        // Open the file
        FileInputStream fstream = new FileInputStream("/Users/gsanchez/Documents/unZipFolder/queries.log");
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String strLine;

         //Read File Line By Line
        while ((strLine = br.readLine()) != null)   {
            Text text = new Text(strLine);
            keywordParser.parseKeyWord(text);
            System.out.println (keywordParser.getKeyWord());
        }

        //Close the input stream
        br.close();
    }
}
