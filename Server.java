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

public class Server {
  public static ServerHandler handler;
  public static ServerThrift.Processor processor;

  public static void main(String [] args) {
    // take the paramenters from command line
    if(args.length != 2) {
      System.out.println("Parameters are not correct: in the format [port] [policy]");
      return;
    }
    int port = Integer.parseInt(args[0]);
    int policy = Integer.parseInt(args[1]);
    if (!(policy == 1 || policy == 2)) {
      System.out.println("Policy input wrong! 1:random policy; 2:load-balancing");
      System.exit(0);
    }

    try {
      handler = new ServerHandler(policy);
      processor = new ServerThrift.Processor(handler);

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

  public static void simple(ServerThrift.Processor processor, int port) {
    try {
      // Create Thrift server socket
      // Server is listening on port 9091
      TServerTransport serverTransport = new TServerSocket(port);
      TTransportFactory factory = new TFramedTransport.Factory();

      //Set server arguments
      TServer.Args args = new TServer.Args(serverTransport);
      args.processor(processor);  //Set handler
      args.transportFactory(factory);  //Set FramedTransport (for performance)

      //Run server as a single thread
      TServer server = new TSimpleServer(args);
      System.out.println("Server is running on port: "+port);
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
