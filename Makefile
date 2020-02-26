all: *.java
		javac -cp ".:/usr/local/Thrift/*" ./*.java -d .
clean:
			rm *.class
