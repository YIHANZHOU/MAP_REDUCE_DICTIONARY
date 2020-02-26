import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;

// Generated code
class ComputeNode {
  public static ComputeHandler handler;
  public static Compute.Processor processor;

  public static void main(String [] args) {
    // take the paramenters from the command line
    if(args.length != 3) {
      System.out.println("Parameters are not correct: in the format [port] [loadP] [delay in millisecond]");
      return;
    }
    int port = Integer.parseInt(args[0]);
    Double loadProbability = Double.parseDouble(args[1]);
    long delay = Integer.parseInt(args[2]);

    try {
        handler = new ComputeHandler(loadProbability, delay);
        processor = new Compute.Processor(handler);

        Runnable simple = new Runnable() {
            public void run() {
                simple(processor, port);
            }
        };

        new Thread(simple).start();
    } catch (Exception x) {
        x.printStackTrace();
    }
  }

  public static void simple(Compute.Processor processor, int port) {
    try {
        //Create Thrift server socket
        TServerTransport serverTransport = new TServerSocket(port);
        TTransportFactory factory = new TFramedTransport.Factory();

        //Set multi-thread server arguments
        TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
        args.processor(processor);
        args.transportFactory(factory);

        //Run server as multi-thread
        TServer server = new TThreadPoolServer(args);
        System.out.println("Compute node running on port: "+port);
        server.serve();
    } catch (Exception e) {
        e.printStackTrace();
    }
  }
}
