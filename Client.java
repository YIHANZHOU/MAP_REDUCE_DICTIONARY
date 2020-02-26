import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;

public class Client {
  public static void main(String [] args) {
    // take the paramenters from the command line
    if(args.length != 3) {
      System.out.println("Parameters are not correct: in the format [ServerIP] [ServerPort] [input_dir]");
      return;
    }
    int port = Integer.parseInt(args[1]);

    //Create client connect.
    try {
      // Server is running on port 9091
      TTransport  transport = new TSocket(args[0], port);
      TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
      ServerThrift.Client client = new ServerThrift.Client(protocol);

      //Try to connect
      transport.open();

      System.out.println("         Processing...");
      //What you need to do.
      String result = client.submitJob(args[2]);

      // the result should be the elaped time and the name of result file
      String[] arr = result.split(";");
      System.out.println("         Job done!");
      System.out.println("         Result file name: "+arr[0]);
      System.out.println("         Elapsed time: "+arr[1]);
    }
    catch(TTransportException e) {
      System.out.println("Server or selected compute node not running. Abort");
      return;
    }
    catch(TException e) {
      System.out.println("Client request fail");
      return;
    }
  }
}
