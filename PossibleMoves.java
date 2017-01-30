/*
 * CS61C Spring 2014 Project2
 * Reminders:
 *
 * DO NOT SHARE CODE IN ANY WAY SHAPE OR FORM, NEITHER IN PUBLIC REPOS OR FOR DEBUGGING.
 *
 * This is one of the two files that you should be modifying and submitting for this project.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PossibleMoves {
    public static class Map extends Mapper<IntWritable, MovesWritable, IntWritable, IntWritable> {
        int boardWidth;
        int boardHeight;
        boolean OTurn;
        /**
         * Configuration and setup that occurs before map gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
            // load up the config vars specified in Proj2.java#main()
            boardWidth = context.getConfiguration().getInt("boardWidth", 0);
            boardHeight = context.getConfiguration().getInt("boardHeight", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);

        }


        /**
         * The map function for the first mapreduce that you should be filling out.
         */
        @Override
        public void map(IntWritable key, MovesWritable val, Context context) throws IOException, InterruptedException {
            /* YOU CODE HERE */
            if(val.getStatus() != 0)
                return;
            int count = 0;
            char turn = OTurn ? 'O' : 'X'; // Getting a character to be placed
            StringBuffer current = new StringBuffer(Proj2Util.gameUnhasher(key.get(), boardWidth, boardHeight)); // Current Board. StringBuffer is more useful to change string

            for(int i = boardWidth * boardHeight - 1 ; i > 0 ; i -= boardHeight) { // From the last colum
                for(int j = i - (boardHeight - 1) ; j <= i ;  j++)             // From the highest row number in the current column  
                    if(current.charAt(j) == ' ') {                             // only if it is empty
                        current.setCharAt(j, turn);  // replace string
                        int child = Proj2Util.gameHasher(new String(current), boardWidth, boardHeight); // Child 
                        current.setCharAt(j, ' ');   // get original string
                        context.write(new IntWritable(child), key); // emit game state / child game state
                        break;
                    }
            }

        }
    }
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, MovesWritable> {

        int boardWidth;
        int boardHeight;
        int connectWin;
        boolean OTurn;
        boolean lastRound;
        /**
         * Configuration and setup that occurs before reduce gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
            // load up the config vars specified in Proj2.java#main()
            boardWidth = context.getConfiguration().getInt("boardWidth", 0);
            boardHeight = context.getConfiguration().getInt("boardHeight", 0);
            connectWin = context.getConfiguration().getInt("connectWin", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
            lastRound = context.getConfiguration().getBoolean("lastRound", true);
        }

        /**
         * The reduce function for the first mapreduce that you should be filling out.
         */
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
            int count = 0; 
            int state = 0; // state is undetermined
            int[] moves = null;

            // To get int array
            ArrayList<Integer> movesTemp = new ArrayList<Integer>();
            
            for(IntWritable value : values){
                movesTemp.add(value.get()); // get value
                count++;
                if (Proj2Util.gameFinished(Proj2Util.gameUnhasher(key.get(), boardWidth, boardHeight), boardWidth, boardHeight, connectWin))
                        state = OTurn ? 1 : 2; // if O turn, 0b01
            }
            if(count != 0) {
                moves = new int[count];
                for (int i = 0; i < movesTemp.size(); i++) 
                    moves[i] = movesTemp.get(i);
            }

            if(lastRound && state == 0) state = 3; // if it is last round and we don't find win, we set to tie
            context.write(key, new MovesWritable(state, 0, moves)); // moveToEnd is not deteremined yet at this phase
        }
    }
}