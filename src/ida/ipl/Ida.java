package ida.ipl;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;



/**
 * 
 * @author Vasileios Christou
 * 
 */
public class Ida {

	public Ibis myIbis = null;

	// joined Ibises
	public IbisIdentifier[] ibises = null;

	static IbisCapabilities capabilities = new IbisCapabilities(
							IbisCapabilities.ELECTIONS_STRICT, 
							IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED, 
							IbisCapabilities.CLOSED_WORLD);

	// winner of election
	public static IbisIdentifier master = null;

	// port type for master's receiveport and workers' sendports (requests/solutions sending - receiving)
	static PortType requestPortType = new PortType(PortType.COMMUNICATION_RELIABLE, 
						  PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT, 
						   PortType.RECEIVE_POLL, PortType.CONNECTION_MANY_TO_ONE);

	
	// port type for master's sendports and workers' receiveport (jobs/orders sending - receiving)
	static PortType replyPortType = new PortType(PortType.COMMUNICATION_RELIABLE, 
						PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT, 
						                         PortType.CONNECTION_ONE_TO_ONE);
	
	// master's receive port for receiving job requests and worker solutions
	private ReceivePort requestRecvPort = null;

	// map of each master sendport to the corresponding worker identifier
	private Map<IbisIdentifier, SendPort> jobSendPorts = 
						new HashMap<IbisIdentifier, SendPort>();

	// worker's send port to send job requests and solutions
	private SendPort requestSendPort = null;

	// worker's receive port to receive jobs and orders
	private ReceivePort jobRecvPort = null;

	private Cube initialCube = null;

	private CubeCache cache = null;

	// the master searches up to this bound before it starts the queue
	private static final int masterBound = 4;

	// queue of jobs created by the master for workers
	private LinkedList<Cube> jobQueue = new LinkedList<Cube>();

	// max and min size of the chunk of cubes in each job
	private static final int maxJobSize = 9;
	private static final int minJobSize = 3;

	// possible orders
	private static final String report = "report";
	private static final String stop = "stop";
	private static final String cont = "continue";

	// request string
	private static final String request = "request";

	public static final boolean PRINT_SOLUTION = false;

	/*-----------------------------------METHODS--------------------------------------*/

	/**
	 * retry connecting the sendport to the target receiveport until connection is successful
	 * @param port
	 * 			sendPort that tries to connect
	 * @param ibis
	 * 			identifier of the worker with the target receivePort
	 * @param targetPortName
	 * 			name of the target receivePort
	 */
	private void connectSendPort(SendPort port, IbisIdentifier ibis, 
				     String targetPortName) throws Exception {
		try {
			port.connect(ibis, targetPortName);
		} catch (ibis.ipl.ConnectionTimedOutException e) {
			// in case of timeout retry
			connectSendPort(port, ibis, targetPortName);
		}
	}


	/**
     * initialize the master send ports and map them to the corresponding workers
     */
    private void sendPortsInitialization() throws Exception {
        for (IbisIdentifier target : ibises) {
            if (!target.equals(myIbis.identifier())) {
                SendPort port = myIbis.createSendPort(replyPortType);
				// map the sendport to the corresponding worker
                jobSendPorts.put(target, port);
				connectSendPort(port, target, "receiveJobPort");
            }
        }
    }


    /**
	 * master method: create the queue of jobs to be sent to the workers for the current bound.
	 * Also send out a job when there are enough cubes in the queue so that the workers don't
	 * have to stay idle until the queue is complete
	 */
	private void createQueue(Cube cube, CubeCache cache) throws IOException {
		
		// if the current bound is reached stop
        if (cube.getTwists() >= cube.getBound()) {
            return;
        }

        // generate all possible cubes from this one by twisting it in
        // every possible way. Gets new objects from the cache
        Cube[] children = cube.generateChildren(cache);
        for (Cube child : children) {
			// when we reach the master bound, start adding cubes to the queue
            if (child.getTwists() >= masterBound) {
                jobQueue.add(child);
				// send out a job when there are workers and enough cubes
                if (ibises.length > 1 && jobQueue.size() >= minJobSize) {
					sendJob();
				}
            } else {
			    // move to next twist recursively
                createQueue(child, cache);
			}
        }
    }


    /**
	 * send a job: If the queue has less than minJobSize cubes ignore requests 
	 * so that small tasks are left for the master. Else check for request.
	 * If there is a request, reply with a maxJobSize job or what is left in the queue.
	 */
    private void sendJob() throws IOException {
		// check for request message
		ReadMessage requestMsg = requestRecvPort.poll();
		// if there was a request
		if (requestMsg != null) {
			String s = requestMsg.readString();		
			// identify the requestor
			IbisIdentifier requestor = requestMsg.origin().ibisIdentifier();
			requestMsg.finish();

			int jobSize = maxJobSize;
			// in not enough cubes send minJobSize to share the last chunk
			if (jobQueue.size() < maxJobSize) {
				jobSize = minJobSize;
			}
			// get the cubes from the queue
			Cube[] job = new Cube[jobSize];
			for (int i = 0; i < jobSize; i++) {
				job[i] = jobQueue.pollFirst();
			}

			// send the job
			SendPort jobSendPort = jobSendPorts.get(requestor);
			WriteMessage jobMsg = jobSendPort.newMessage();
			jobMsg.writeObject(job);
			jobMsg.finish();
	
			// put the job cubes in the cache
			for (Cube cube : job) {
				cache.put(cube);
			}
		}   
    }


	/**
	 * Recursive function to find a solution for a given cube. Only searches to
	 * the bound set in the cube object.
	 * 
	 * @param cube
	 *            cube to solve
	 * @param cache
	 *            cache of cubes used for new cube objects
	 * @return the number of solutions found
	 */
	public int solutions(Cube cube, CubeCache cache) throws Exception {
		if (cube.isSolved()) {
			return 1;
		}

		if (cube.getTwists() >= cube.getBound()) {
			return 0;
		}
		// master sends jobs while searching, only 
		// if there are enough cubes (and if there are workers)
		if (master.equals(myIbis.identifier()) && jobQueue.size() > minJobSize 
											   && ibises.length > 1) {
			sendJob();
		}

		// generate all possible cubes from this one by twisting it in
		// every possible way. Gets new objects from the cache
		Cube[] children = cube.generateChildren(cache);

		int result = 0;

		for (Cube child : children) {
			// recursion step
			int childSolutions = solutions(child, cache);
			if (childSolutions > 0) {
				result += childSolutions;
				if (PRINT_SOLUTION) {
					child.print(System.err);
				}
			}
			// put child object in cache
			cache.put(child);
		}
		return result;
	}


	/**
     * receive the last job request from each worker and when all workers
     * have sent a request, send out the orders to report their solutions
     */
    private void sendOrders() throws Exception {
        ArrayList<WriteMessage> ordersArray = new ArrayList<WriteMessage>();
        
		for (int i = 0; i < ibises.length - 1; i++) {
			// receive last job request message
            ReadMessage requestMsg = requestRecvPort.receive();
            requestMsg.readString();
			// identify the requesting worker
            IbisIdentifier worker = requestMsg.origin().ibisIdentifier();
            requestMsg.finish();
			// get the port corresponding to the requestor from the map
			SendPort sendOrdersPort = jobSendPorts.get(worker);
			// write the report message
			WriteMessage orders = sendOrdersPort.newMessage();
			orders.writeString(report);
			ordersArray.add(orders);
        }
        // send out messages only after receiving last request from every worker
        for (WriteMessage ordersMsg : ordersArray) {
            ordersMsg.finish();
        }
    }


    /**
     * receive solutions from the workers
     * 
     * @return the sum of solutions for this bound
     */
    private int receiveWorkerSolutions() throws Exception {
        int solutionsFound = 0;
        for (int i = 0; i < ibises.length - 1; i++) {
            ReadMessage resultMsg = requestRecvPort.receive();
            int solutions = resultMsg.readInt();
            resultMsg.finish();
            solutionsFound += solutions;
        }
        return solutionsFound;
    }
    

	/**
	 * send the second type of orders to every worker
	 *
	 * @param orders
     *            to continue or stop working
	 */
	private void sendSecondOrders(String orders) throws Exception {

		for (IbisIdentifier ibis : ibises) {
			if (!ibis.equals(myIbis.identifier())) {
				SendPort sendOrdersPort = jobSendPorts.get(ibis);
				WriteMessage msg = sendOrdersPort.newMessage();
				msg.writeString(orders);
				msg.finish();
			}
		}
    }

	
    /**
     * master's procedure: initialize ports and then start sequential IDA* until
	 * the masterbound. After this create queue of jobs and send to workers whilst
	 * also working on some cubes. When queue is empty ask for solutions, receive them
	 * and order the workers to stop or continue to the next iteration
     */
    public void masterProcess() throws Exception {
        try {
            // send port for receiving job requests
            requestRecvPort = myIbis.createReceivePort(requestPortType,
												      "receiveRequestPort");
            requestRecvPort.enableConnections();
			// one-to-one send ports to send jobs to workers
            sendPortsInitialization();
            
            // start time
            long start = System.currentTimeMillis();
            
			int bound = 0;
			int result = 0;
			Cube nextCube = null;
			System.out.print("Bound now:");
			
			// each iteration corresponds to one bound
			while (result == 0) {
				//increase current bound
				bound++;
				initialCube.setBound(bound);
				System.out.print(" " + bound);
				
				if (bound <= masterBound) {
					// try to solve the cube until masterbound
					result = solutions(initialCube, cache);
					if (result > 0) {
						// if solution is found send orders to stop waiting
						// so that all ibises end normally
						sendOrders();
						sendSecondOrders(stop);
					}
				} else {
					// create and send jobs after masterbound
					createQueue(initialCube, cache);
					// start working while also checking for requests
					while (!jobQueue.isEmpty()) {
						nextCube = jobQueue.pollFirst();
						result += solutions(nextCube, cache);
						cache.put(nextCube);
					}
					// when queue is emptied
					// send 1st orders to all workers to report their solutions
					sendOrders();
					//receive solutions for this bound
					result += receiveWorkerSolutions();
					
					// if no solutions are found send orders to continue
					if (result > 0) {
						// if solution is found send orders to stop
						sendSecondOrders(stop);
					} else {
						sendSecondOrders(cont);
					}
				}
			}
			// stop time
            long end = System.currentTimeMillis();

			// print results
			System.out.println();
			System.out.println("Solving cube possible in " + result + " ways of "
					+ bound + " steps");
			
            // NOTE: this is printed to standard error! The rest of the output is
        	// constant for each set of parameters. Printing this to standard error
        	// makes the output of standard out comparable with "diff"
            System.err.println("ida took " + (end - start)
                    + " milliseconds");

        } catch (Exception e) {
            System.err.println(e + " in masterProcess");
        }
    }


	/**
	 * Check the orders the master sent, send my solutions to master.
	 * @param message 
	 * 				the master's report orders
	 * @param solutionsFound 
	 * 				worker's number of solutions for this bound
	 * @return true if work is done, else return false
	 */
	private boolean checkOrders(String message, int solutionsFound) throws Exception {
		boolean done = false;
		// orders to report solutions to master
		if (message.equals(report)) {
			
			// reply with solutions
			WriteMessage resultMsg = requestSendPort.newMessage();
			resultMsg.writeInt(solutionsFound);
			resultMsg.finish();

			// receive my next move from master (stop or continue working)
			ReadMessage nextMoveMsg = jobRecvPort.receive(); 
			String nextMove = nextMoveMsg.readString();
			nextMoveMsg.finish();
			// if nextMove is stop, end the work loop
			if (nextMove.equals(stop)) {
				done = true;
			// the other case should be to continue working
			} else if (!nextMove.equals(cont)) {
				// something is wrong
				System.err.println("Didn't receive second orders!");
			}
		//wrong orders
		} else {
			System.err.println("Didn't receive first orders!");
		}

		return done;
	}


	/**
	 * worker's procedure: initialize ports and then send job request to master, receive reply. 
	 * If the reply contains cubes, work on them. If the reply contains an orders string, check
	 * the orders (send solutions to master and continue to the next bound or stop, depending on the master's call)
	 */
	public void workerProcess() throws Exception {
		
		// create send port for requests and solutions
		requestSendPort = myIbis.createSendPort(requestPortType);
		requestSendPort.connect(master, "receiveRequestPort");

		// create receive port for jobs and orders
		jobRecvPort = myIbis.createReceivePort(replyPortType, "receiveJobPort");
		jobRecvPort.enableConnections();
		
		//start requesting and working

		// flag to check if solution has been found
		boolean done = false;
		int solutionsFound = 0;

		while (!done) {

			// send job request to master
			WriteMessage requestMsg = requestSendPort.newMessage();
			requestMsg.writeString(request);
			requestMsg.finish();

			// receive the new job or report orders
			ReadMessage replyMsg = jobRecvPort.receive();
			Object job = replyMsg.readObject();
			replyMsg.finish();
			
			try {
				// check if the message includes cubes
				Cube[] cubes = (Cube[]) job;
				
				// search the cubes for solutions
				for (Cube cube : cubes) {
					solutionsFound += solutions(cube, cache);
				}
			// report orders string received instead of cubes
			} catch (ClassCastException exc) {
				// follow the orders
				done = checkOrders((String) job, solutionsFound);
			}
		}
	}


	public static void printUsage() {
        System.out.println("Rubiks Cube solver");
        System.out.println("");
        System.out
                .println("Does a number of random twists, then solves the Rubiks cube with a simple");
        System.out
                .println(" brute-force approach. Can also take a file as input");
        System.out.println("");
        System.out.println("USAGE: Ida [OPTIONS]");
        System.out.println("");
        System.out.println("Options:");
        System.out.println("--size SIZE\t\tSize of cube (default: 3)");
        System.out
                .println("--twists TWISTS\t\tNumber of random twists (default: 11)");
        System.out
                .println("--seed SEED\t\tSeed of random generator (default: 0");
        System.out
                .println("--file FILE_NAME\t\tLoad cube from given file instead of generating it");
        System.out
                .println("--printfile FILE_NAME\t\tWrite initial cube to given file");
        System.out.println("");
    }

	
	/**
	 * Initializes all ibises and elects a master
	 */
	private void joinAndElectMaster() throws Exception {
		// create ibis
		myIbis = IbisFactory.createIbis(capabilities, null, replyPortType, requestPortType);
		// wait until all ibises have joined the pool
		myIbis.registry().waitUntilPoolClosed();
		// list of the ibises
		ibises = myIbis.registry().joinedIbises();
		// elect a master
		master = myIbis.registry().elect("master");
	}

	
	/**
	 * run the whole procedure
	 */
	private void run(String[] arguments) throws Exception {
		
		// wait for all ibises to join and elect a master
		joinAndElectMaster();

		// default parameters of puzzle
		int size = 3;
        int twists = 11;
        int seed = 0;
        String fileName = null;
		String printFileName = null;

        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i].equalsIgnoreCase("--size")) {
                i++;
                size = Integer.parseInt(arguments[i]);
            } else if (arguments[i].equalsIgnoreCase("--twists")) {
                i++;
                twists = Integer.parseInt(arguments[i]);
            } else if (arguments[i].equalsIgnoreCase("--seed")) {
                i++;
                seed = Integer.parseInt(arguments[i]);
            } else if (arguments[i].equalsIgnoreCase("--file")) {
                i++;
                fileName = arguments[i];
			} else if (arguments[i].equalsIgnoreCase("--printfile")) {
                i++;
                printFileName = arguments[i];
            } else if (arguments[i].equalsIgnoreCase("--help") || arguments[i].equalsIgnoreCase("-h")) {
				// only master prints
				if (master.equals(myIbis.identifier())) {
                	printUsage();
                	System.exit(0);
				}
            } else {
				if (master.equals(myIbis.identifier())) {
                	System.err.println("unknown option : " + arguments[i]);
                	printUsage();
                	System.exit(1);
				}
            }
        }

		cache = new CubeCache(size);

		if (master.equals(myIbis.identifier())) {
			// create cube
			if (fileName == null) {
				initialCube = new Cube(size, twists, seed);
				System.out.println("Searching for solution for cube of size "
					+ size + ", twists = " + twists + ", seed = " + seed);
			} else {
				try {
					initialCube = new Cube(fileName);
					System.out.println("Searching for solution for cube from file " + new File(fileName).getName());
				} catch (Exception e) {
					System.err.println("Cannot load cube from file: " + e);
					System.exit(1);
				}
			}

			// print cube info
			initialCube.print(System.out);
			System.out.flush();
			if (printFileName != null) {
				try {
					initialCube.print(printFileName + "-" + twists + "-" + seed);
				} catch(Exception e) {
					System.err.println("Cannot write cube to file: " + e);
					System.exit(1);
				}
			}

			// start the master procedures
			masterProcess();
		} else {
			// start the worker procedures
			workerProcess();
		}
		// leave pool
		myIbis.end();
	}
	

	/**
	 * Main function.
	 * 
	 * @param arguments
	 *            list of arguments
	 */
	public static void main(String[] arguments) {
		try {
			new Ida().run(arguments);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
}
