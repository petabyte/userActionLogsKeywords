import org.apache.hadoop.io.Text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gsanchez on 2/24/16.
 */
public class KeywordParser {
    private String keyWord = null;

    public void parseKeyWord(Text lineRecordText){
           keyWord = null;
           String lineRecord = lineRecordText.toString();
           String [] lineSplit = lineRecord.split("\\t+");
           String firstElement = lineSplit[1];
           if( firstElement != null && firstElement.length() > 0 ){
               keyWord = firstElement.replaceAll("\\s+"," ");
               keyWord = keyWord.replaceAll("^\"\\s", "\"");
               keyWord = keyWord.replaceAll("^\"\"", "\"");
               keyWord = keyWord.replaceAll("\\s\"$","\"");
               keyWord = keyWord.replaceAll("\"\"$","\"");
           }
    }

    public boolean foundKeyWord(){
        if(keyWord != null && keyWord.length() > 0){
            return true;
        }
        return false;
    }

    public String getKeyWord(){
        return keyWord;
    }
}
