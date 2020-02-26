import org.apache.thrift.TException;
import java.io.*;
import java.util.*;
import java.lang.Math;

public class ComputeHandler implements Compute.Iface
{
  private Double loadProbability;
  private long delay;
  private int num;  // number of total map tasks accepted by this node
  private static final Object lock = new Object();  // lock for num
  // private int num1;  // debug use (count number of injections)
  // private static final Object lock1 = new Object();  // debug use (lock for num1)
  // Constructor with parameters
  ComputeHandler(Double loadProbability, long delay) {
    this.loadProbability = loadProbability;
    this.delay = delay;
    num = 0;
    // num1 = 0;  // debug
  }

  @Override
  public int handleMap(String inputFile, int policy) {
    // based on the policy, determine if reject the task
    double randomDouble;
    int count;  // local count of # of the map task
    if(policy == 2) {
      // random generate the number to see if is < probability
      randomDouble = Math.random();  // [0.0,1.0)
      if(randomDouble < loadProbability) {  // reject it!
        System.out.println("  Reject a map task!!");
        return 2;
      }
    }

    synchronized(lock) {  // increment the count of map tasks
      num++;
      count = num;
    }
    System.out.println("#"+count+" map tasks accepted by this node; filename:"+inputFile);
    // based on the probability, determine if inject the delay.
    randomDouble = Math.random();  // [0.0,1.0)
    if(randomDouble < loadProbability) {
      // synchronized(lock1) {
      //   num1++;
      // }
      // System.out.println("  Load injection:"+delay+"milliseconds");
      try {
        Thread.sleep(delay);
      } catch (Exception e) {
        e.printStackTrace();
        return 3;
      }
    }

    // then do the word count and calculate the sentiment score
    // define the delimiter
    String delimiter = "[^-a-zA-Z]+";
    // create the hashset
    Set<String> setPositive = new HashSet<>();
    Set<String> setNegative = new HashSet<>();

    try {
      // add the nagative and positive words into the hashset
      Scanner scNeg = new Scanner(new File("negative.txt"));
      while(scNeg.hasNext()) {
        setNegative.add(scNeg.next());
      }
      Scanner scPos = new Scanner(new File("positive.txt"));
      while(scPos.hasNext()) {
        setPositive.add(scPos.next());
      }

      // count the negative and positive word of the input file
      Scanner scInput = (new Scanner(new BufferedReader(new FileReader("input_dir/"+inputFile)))).useDelimiter(delimiter);
      int countPos = 0;
      int countNeg = 0;
      while(scInput.hasNext()) {
        String words = scInput.next();
        String[] word=words.split("--");
          for(int i=0;i<word.length;i++){
        if(setPositive.contains(word[i].toLowerCase())) {
          //System.out.println(word[i].toLowerCase());
          countPos++;
        }if(setNegative.contains(word[i].toLowerCase())) {
          //System.out.println(word.toLowerCase());
          countNeg++;
        }}
      }
      // calculate the sentiment score
      Double score = (countPos-countNeg+0.0)/(countPos+countNeg);

      // generate the intermediate file and store the <filename, score> pair into it
      String newFileName = "intermediate_dir/"+"inter_"+inputFile;
      File interFile = new File(newFileName);
      // create a new file. Overwrite if exist
      PrintWriter writer = new PrintWriter(interFile);
      // print the <filename:score> pair into the intermediate file
      writer.println(inputFile+":"+score);

      writer.close();
      scNeg.close();
      scPos.close();

      System.out.println("#"+count+" Map task done!");
    }
    catch (Exception x) {
      System.out.println("Error: executing word counting fail");
      return 3;  // return fail
    }
    // job finished return success
    return 1;
  }

  @Override
  public String handleSort(String interFileDir) {  // return the name of the reuslt file
    System.out.println("Sort task received!");
    // read info from each intermdediate file into list
    File folder = new File(interFileDir);  // this is hardcode or pass by parameter?
    File[] listOfFiles = folder.listFiles();
    if (listOfFiles == null) {
      System.out.println("Error message: sort phase - input file dir not exist");
      return null;
    }
    int numOfFiles = 0;
    List<Entry> list = new ArrayList<Entry>();
    // traverse all the file entries
    for (int i = 0; i < listOfFiles.length; i++) {
      if (listOfFiles[i].isFile()) {
        numOfFiles++;
        // open this file and read the number
        String delimiter = ":";
        try {
          Scanner sc = (new Scanner(new File("intermediate_dir/"+listOfFiles[i].getName()))).useDelimiter(delimiter);
          Entry entry = new Entry(sc.next(), Double.parseDouble(sc.next()));
          list.add(entry);
        } catch (Exception e) {
          System.out.println("Error message: Scan the intermediate files error");
          return null;
        }
      }
    }
    // sort this list
    Collections.sort(list);

    // print the result into the result file
    String newFileName = "output_dir/result";
    File resultFile = new File(newFileName);
    try {
      // create a new file. Overwrite if exist
      PrintWriter writer = new PrintWriter(resultFile);
      // print the result into the result file
      for(int i=0; i<list.size(); i++) {
        // System.out.println("       "+list.get(i).getScore());
        writer.println(list.get(i).getFileName()+":"+list.get(i).getScore());
      }
      writer.close();
    } catch(Exception e) {
      System.out.println("Error message: writing output file fail");
      return null;
    }
    System.out.println("Sort task done!");
    return "output_dir/result";
  }

  // define structures to prepare for sort()
  private class Entry implements Comparable<Entry>{
    private String fileName;
    private Double score;
    public Entry(String fileName, Double score) {
      this.fileName = fileName;
      this.score = score;
    }
    // getter and setter
    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }
    public Double getScore() { return score; }
    public void setScore(Double score) { this.score = score; }
    @Override
    public int compareTo(Entry en) {
      return score < en.getScore() ? 1 : -1;
    }
  }

}
