import java.io.*;
import java.util.*;

public class MySort
{


    public static void main(String args[])
    {
        //check for input
        if(args.length<2)
        {
            System.out.println("Usage [input file] [output file]");
            return;
        }

        try
        {
            String input_file = args[0];
            String output_file = args[1];
            int r_n = 4;
            long startTime = System.nanoTime();



            File input = new File(input_file);
            long bs = calculateBlockSize(input);


            final BufferedReader br = new BufferedReader(new FileReader(input));

            final List<File> tmpfiles = new ArrayList<>();
            myFileReader threads [] = new myFileReader[r_n];

            //read the input file by spawning 4 threads
            for(int i = 0;i<r_n;i++)
            {
                threads[i] = new myFileReader(br,bs , tmpfiles,i);
                threads[i].start();

            }
            for(int i = 0;i<r_n;i++)
            {
                threads[i].join();
            }

            System.out.println("Generated "+tmpfiles.size()+" batch files.");
            long freemem = Runtime.getRuntime().freeMemory();
            System.out.println("Free memory during merge is "+freemem/1024+ " KB.");

            mergetmpFiles(tmpfiles,output_file,chunk_merge_counter_calculator(freemem,tmpfiles.size()));

            long endTime   = System.nanoTime();
            long totalTime = endTime - startTime;
            System.out.println("Time required for MySort is "+totalTime/1000000000.0+" seconds. \n");

        }
        catch (NumberFormatException e)
        {
            System.out.print("Provided input (3rd or 4th or 5th ) in incorrect");
        }
        catch (FileNotFoundException e)
        {
            System.out.print("Provided input (1st or 2nd) in incorrect");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    //calculate how many chunk files should be merged at once
    private static int chunk_merge_counter_calculator(long free_mem, int size)
    {

        if(size * 1024 * 8 > free_mem)
        {
            return chunk_merge_counter_calculator(free_mem,size/2);
        }
        else
        {
            System.out.println("Merge counter is "+size);
            return size;
        }
    }

    //Calculate the max file size for the chunk
    private static long calculateBlockSize(File file)
    {
        long sizeoffile = file.length();
        //Assuming max 1000 chunk files will be generated
        long blocksize = sizeoffile / 1000;
        long freemem = Runtime.getRuntime().freeMemory();
        System.out.println("Free memory found on the system is "+freemem/1024+ " KB.");


        if( blocksize < freemem/4)
        {
            blocksize = freemem/2;
        }
        else if(blocksize >= freemem)
        {
            System.err.println("We may run out of memory.");
        }
        System.out.println("Block size for reading is "+blocksize/1024+" KB");
        return blocksize;
    }

    //merge the chunk files, merge count describes, how many files should be merged at once.
    private static void mergetmpFiles(List<File> tmpfiles, String outfile, int merge_count)
    {
        if(tmpfiles.size() <= merge_count)
        {
            try
            {
                File out_file = new File(outfile);
                myFileMerger t = new myFileMerger(tmpfiles,out_file);
                t.start();
                t.join();
                System.out.println("Sorting completed! Wrote output in " + outfile);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

        }
        else
        {
            int c = 0;
            List<File> newtmpFiles = new ArrayList<>();
            List<myFileMerger> merger = new ArrayList<>();

            int t = 0;
            //spawn the merger thread for the batch of tmp files
            while(c!=1)
            {

                List<File> files_to_merge = new ArrayList<>();
                if(tmpfiles.size()> merge_count)
                {
                    files_to_merge = tmpfiles.subList(0, merge_count);
                    tmpfiles = tmpfiles.subList(merge_count, tmpfiles.size());
                }
                else if(tmpfiles.size() ==1)
                {
                    newtmpFiles.add(tmpfiles.get(0));
                    break;
                }
                else
                {
                    files_to_merge = tmpfiles;
                    c = 1;
                }

                try
                {
                    t++;
                    System.out.println("Merging stage "+t+".");
                    File tmp_merge_file = File.createTempFile("FileChunkMerge"+t,"Thread.tmp");
                    tmp_merge_file.deleteOnExit();
                    myFileMerger th = new myFileMerger(files_to_merge,tmp_merge_file);
                    merger.add(th);
                    newtmpFiles.add(tmp_merge_file);
                    th.start();

                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }

            }

            c=0;
            for(myFileMerger m: merger)
            {
                try
                {
                    m.join();
                    c++;
                }
                catch (Exception e)
                {

                }

            }
            mergetmpFiles(newtmpFiles,outfile,merge_count);
        }
    }

    //save file chunk as temporary file
    static File saveTmpFile( List<String> lines)
    {
        Collections.sort(lines, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });

        BufferedWriter fbw;
        try {
            File tmpfile = File.createTempFile("FileChunk","Thread.tmp");
            tmpfile.deleteOnExit();

            fbw = new BufferedWriter(new FileWriter(tmpfile),1024 * 8);
            for(String r : lines) {
                fbw.write(r+"\r\n");
            }
            fbw.close();
            return tmpfile;
        }
        catch (Exception e)
        {
            return null;
        }
    }

    //This class allows to read file concurrently
    static class myFileReader extends Thread
    {
        BufferedReader br;
        long max_capacity;
        List<File> file_list;
        int thread_no;

        private myFileReader(BufferedReader br, long max, List<File> file_list, int thread_no)
        {
            this.br = br;
            max_capacity = max;
            this.file_list = file_list;
            this.thread_no = thread_no;
        }

        @Override
        public void run()
        {
            String line = "";
            List<String> lines = new ArrayList<>();
            long c = 0;
            try
            {

                while(line != null)
                {
                    synchronized (br)
                    {
                        line = br.readLine();
                        if(line != null && !line.equals(""))
                        {
                            c += line.length() + 20; //length + overhead
                            lines.add(line);

                            if(c >= max_capacity)
                            {
                                File tmpfile = saveTmpFile(lines);
                                c =0;
                                lines.clear();
                                synchronized (file_list)
                                {
                                    file_list.add(tmpfile);
                                }
                            }

                        }
                    }
                }

                if(lines.size() > 0)
                {
                    File tmpfile = saveTmpFile(lines);
                    lines.clear();
                    synchronized (file_list)
                    {
                        file_list.add(tmpfile);
                    }
                }

            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    //this class allows to merge batch of files concurrently
    static class myFileMerger extends Thread
    {
        List<File> tmpfiles;
        File outfile;

        private myFileMerger(List<File> tmpfiles, File outfile)
        {
            this.tmpfiles = tmpfiles;
            this.outfile = outfile;
        }

        @Override
        public void run() {
            super.run();

            PriorityQueue<TmpFileBuffer> pq = new PriorityQueue<TmpFileBuffer>(11,
                    new Comparator<TmpFileBuffer>() {
                        public int compare(TmpFileBuffer i, TmpFileBuffer j) {
                            String a = i.peek();
                            String b = j.peek();
                            if(a == null || b == null)
                            {
                                return 0;
                            }
                            else
                            {
                                return  a.compareTo(b);
                            }

                        }
                    }
            );

            try
            {
                for (File f : tmpfiles) {
                    pq.add(new TmpFileBuffer(f));
                }

                BufferedWriter writer = new BufferedWriter(new FileWriter(outfile));
                while(pq.size()>0)
                {
                    TmpFileBuffer tfb = pq.poll();
                    String r = tfb.pop();
                    writer.write(r +"\r\n");
                    if(tfb.empty())
                    {
                        tfb.close();
                        tfb.originalfile.delete();
                    }
                    else
                    {
                        pq.add(tfb);
                    }
                }
                writer.close();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }


}

//This class opens the tmp files and reads the line in safer way
class TmpFileBuffer
{
    public BufferedReader fbr;
    public File originalfile;
    private String myLine;
    private boolean empty;

    public TmpFileBuffer(File f) throws IOException {
        originalfile = f;
        fbr = new BufferedReader(new FileReader(f), 1024 * 8);
        fetch();
    }

    public boolean empty() {
        return empty;
    }

    private void fetch() throws IOException {
        try {
            if((this.myLine = fbr.readLine()) == null){
                empty = true;
                myLine = null;
            }
            else{
                empty = false;
            }
        } catch(EOFException oef) {
            empty = true;
            myLine = null;
        }
    }

    public void close() throws IOException {
        fbr.close();
    }


    public String peek() {
        if(empty()) return null;
        return myLine.toString();
    }
    public String pop() throws IOException {
        String answer = peek();
        fetch();
        return answer;
    }



}
