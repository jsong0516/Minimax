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

public class SolveMoves {
    public static class Map extends Mapper<IntWritable, MovesWritable, IntWritable, ByteWritable> {
        /**
         * Configuration and setup that occurs before map gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
        }

        /**
         * The map function for the second mapreduce that you should be filling out.
         */
        @Override
        public void map(IntWritable key, MovesWritable val, Context context) throws IOException, InterruptedException {
            // We takes input from both This mapreduce context and Possible Move context
            // If it doesn't exist in child/parent pair, 




            int[] moves = val.getMoves();
            int len = moves.length;
            for(int i = 0; i < len; i++)
                context.write(new IntWritable(moves[i]), new ByteWritable((byte)val.getValue())); // emitting parent and child byte status

        }
    }

    public static class Reduce extends Reducer<IntWritable, ByteWritable, IntWritable, MovesWritable> {

        int boardWidth;
        int boardHeight;
        int connectWin;
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
            connectWin = context.getConfiguration().getInt("connectWin", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
        }


        
        /**
         * The reduce function for the first mapreduce that you should be filling out.
         */
        @Override
        public void reduce(IntWritable key, Iterable<ByteWritable> values, Context context) throws IOException, InterruptedException {
            // If it O turn, winning value for O
            int tie = 3;
            int value_looking = OTurn? 1 : 2 ; // O Win = 1, X Win = 2, 3 is for Tie
            int opponent_win = OTurn? 2 : 1;
            int best = 252 | opponent_win; // 0b 11111100 opponent win
            boolean validParent = false; //edited
            boolean lastLevel = true;

            for(ByteWritable value : values) { // This will get the best values out of children board
                int val = value.get() & 255;
                if ((val & 3) == value_looking) { // If val is winning
                    if ((best & 3) != value_looking) best = val;
                    else best = (best >> 2) < (val >> 2) ? best : val;
                } else if (((val & 3) == tie) && ((best & 3) != value_looking)) { // If val is tie 
                    if((best & 3) == tie) best = (best >> 2) > (val >> 2) ? best : val; // if they both tie
                    else best = val;
                } else if (((val & 3) == opponent_win) && ((best & 3) == opponent_win)) // if oppopentwin and if and only if our best is also opponentWin
                    best = (best >> 2) > (val >> 2) ? best : val;

                if ((val & 3) == 0) { // edited
                    validParent = true;
                }
                if ((val >> 2) != 0) {
                    lastLevel = false;
                }
            }
            validParent = lastLevel? true: false;
            // Now, "best" has the best move
            if (!validParent) return; // edited
            ArrayList<IntWritable> moves = construct_vaild_tree(key);// Create final nodes
            int[] parent_moves = new int[moves.size()];
            for(int i = 0 ; i < moves.size(); i++)
                parent_moves[i] = moves.get(i).get();

            context.write(key, new MovesWritable((byte)(best + 8), parent_moves)); // final

        }

        private ArrayList<IntWritable> construct_vaild_tree(IntWritable child){ // context.Write Valid Parent board
            ArrayList<IntWritable> moves = new ArrayList<IntWritable>();
            char turn = OTurn ? 'O' : 'X'; // Getting a character to be placed
            StringBuffer current = new StringBuffer(Proj2Util.gameUnhasher(child.get(), boardWidth, boardHeight));

            for(int i = boardWidth * boardHeight - 1 ; i > 0 ; i -= boardHeight) { // From the last colum
                for(int j = i ; j >= (i - (boardHeight - 1)) ;  j--) { // From very top of that column
                    if(current.charAt(j) == turn) {
                        //if(false) break;// speical case -> break *********************************************************
                        current.setCharAt(j, ' ');// generate parent board by removing a character
                        moves.add(new IntWritable(Proj2Util.gameHasher(new String(current), boardWidth, boardHeight)));          // write parent, ByteValu pair
                        current.setCharAt(j, turn); // go back
                        break;
                    }
                    else if(current.charAt(j) != ' ') // if it is opponent character
                        break;   
                }
            }
            return moves;
        }


    }
}
