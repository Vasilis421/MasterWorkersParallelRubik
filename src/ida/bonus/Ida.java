package ida.bonus;

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
import java.util.Random;


/**
 * 
 * @author Vasileios Christou
 * 
 */
public class Ida {

	public Ibis myIbis = null;

	// joined ibises
	public IbisIdentifier[] ibises = null;

	static IbisCapabilities capabilities = new IbisCapabilities(
							IbisCapabilities.ELECTIONS_STRICT,
							IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED, 
							IbisCapabilities.CLOSED_WORLD);
	
	// winner of election
	public IbisIdentifier master = null;

	// type for sending-receiving normal jobs and orders to/by the workers
	static PortType oneToOneType = new PortType(PortType.COMMUNICATION_RELIABLE,
							PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT, 
							PortType.RECEIVE_POLL, PortType.RECEIVE_TIMEOUT, 
							PortType.CONNECTION_ONE_TO_ONE);

	// type for everything else (send-receive requests/solutions to/by the master 
	// and whole stealing communication)
	static PortType manyToOneType = new PortType(PortType.COMMUNICATION_RELIABLE, 
							PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT, 
							PortType.RECEIVE_POLL, PortType.RECEIVE_TIMEOUT, 
							PortType.CONNECTION_MANY_TO_ONE);
	
	// master's receive port for receiving job requests and worker solutions
	private ReceivePort requestRecvPort = null;

	// map of each master sendport to the corresponding worker identifier
	private Map<IbisIdentifier, SendPort> jobSendPorts = 
						new HashMap<IbisIdentifier, SendPort>();

    // worker's send port to send job requests and solutions to master
	private SendPort requestSendPort = null;

	// worker's receive port to receive jobs and orders from master
	private ReceivePort jobRecvPort = null;
	
	/* ------------------ NEW PORTS FOR STEALING ------------------------- */
	// map of each worker sendport to another worker's identifier
	// this sendports are used to send both steal requests and steal replies
	private Map<IbisIdentifier, SendPort> stealSendPorts = 
						new HashMap<IbisIdentifier, SendPort>();

	// worker's receive port to receive steal requests and stolen jobs
	private ReceivePort stealRecvPort = null;
	/* ------------------------------------------------------------------- */

	private Cube initialCube = null;

	private CubeCache cache = null;
	
	// the master searches up to this bound before it starts the queue
	private static final int masterBound = 4;

	// personal queue of jobs 
	private LinkedList<Cube> jobQueue = new LinkedList<Cube>();

	// max and min size of the chunk of cubes in each job
	private static final int maxJobSize = 9;
	private static final int minJobSize = 3;

	// possible orders
	private static final String report = "report";
	private static final String stop = "stop";
	private static final String cont = "continue";

	// request strings
	private static final String request = "request";
	private static final String steal = "steal";

	public static final boolean PRINT_SOLUTION = false;

	// --------------------------- METHODS -------------------------------------------

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
    * initialize the send ports and map them to the corresponding workers
    */
    private void sendPortsInitialization() throws Exception {
        for (IbisIdentifier target : ibises) {
	    	// only connect with workers
            if (!target.equals(myIbis.identifier()) && !target.equals(master)) {
				// worker connects with other workers for work stealing
                if (!master.equals(myIbis.identifier())) {
					SendPort stealPort = myIbis.createSendPort(manyToOneType);
					// map the sendport to the corresponding worker	
					stealSendPorts.put(target, stealPort);
					connectSendPort(stealPort, target, "receiveStolenJobPort");
				// master connects with all workers to send out jobs and orders
				} else {
					SendPort replyPort = myIbis.createSendPort(oneToOneType);
					// map the sendport to the corresponding worker
					jobSendPorts.put(target, replyPort);
					connectSendPort(replyPort, target, "receiveJobPort");
				}
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
				// send out a job
                sendJob();
            } else
				// move to next twist recursively
                createQueue(child, cache);
        }
    }


    /**
     * send a job: If the queue has less than minJobSize cubes ignore requests 
	 * so that small tasks are left for the master. Else check for request.
	 * If there is a request, reply with a maxJobSize job or what is left in the queue.
	 */
    private void sendJob() throws IOException {
        // only master checks the size of the queue before sending a job
        if ((jobQueue.size() >= minJobSize || !master.equals(myIbis.identifier())) && ibises.length > 1) {
			ReadMessage requestMsg = null;
			//master checks for job requests
			if (master.equals(myIbis.identifier())) {
				requestMsg = requestRecvPort.poll();
			//worker checks for steal requests
			} else {
				requestMsg = stealRecvPort.poll();
			}
			//check if there was a request
			if (requestMsg != null) {
				try {
					String s = requestMsg.readString();
					// identify the requestor
					IbisIdentifier requestor = requestMsg.origin().ibisIdentifier();
					requestMsg.finish();
					int jobSize;
					// master 
					if (master.equals(myIbis.identifier())) {
						// send maximum chunk size
						if (jobQueue.size() >= maxJobSize) {
							jobSize = maxJobSize;
						// if not enough cubes send minimum
						} else {
							jobSize = minJobSize;
						}
					// worker sends half its queue to the thief
					} else {
						jobSize = jobQueue.size() / 2;
					}
					// get the cubes from the queue
					Cube[] job = new Cube[jobSize];
					for (int i = 0; i < jobSize; i++) {
						job[i] = jobQueue.pollFirst();
					}
					// send the job
					SendPort jobSendPort = null;
					if (master.equals(myIbis.identifier())) {
						jobSendPort = jobSendPorts.get(requestor);
					} else {
						jobSendPort = stealSendPorts.get(requestor);
					}
					WriteMessage jobMsg = jobSendPort.newMessage();
					jobMsg.writeObject(job);
					jobMsg.finish();
					// put the job cubes in the cache
					for (Cube cube : job) {
						cache.put(cube);
					}
				} catch (Exception e) {
					// in case a job is received instead of request
					// could happen in simultaneous stealing
				}
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
		
		// master sends jobs while searching
		if (master.equals(myIbis.identifier())) {
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
     * receive the last job request from each worker, send report orders, 
	 * receive solutions, until all workers have sent their solutions
     */
    private int sendOrders() throws Exception {
        // list to keep track of the workers that have sent their solutions
		ArrayList<IbisIdentifier> solutionsSent = new ArrayList<IbisIdentifier>();
		int solutionsFound = 0;
		// repeat until all workers have sent their solutions
		while (solutionsSent.size() < ibises.length - 1) {
			// receive last request or solutions message from workers
			ReadMessage requestMsg = requestRecvPort.receive();
			// identify the sender
			IbisIdentifier worker = requestMsg.origin().ibisIdentifier();
			try {
				// if it is a request message
				String s = requestMsg.readString();
				requestMsg.finish();
				// reply with report orders
				SendPort reportSendPort = jobSendPorts.get(worker);
				WriteMessage reportMsg = reportSendPort.newMessage();
				reportMsg.writeString(report);
				reportMsg.finish();
			} catch (java.io.OptionalDataException e) {
				// if it is a solutions message add them to sum
				solutionsFound += requestMsg.readInt();
				requestMsg.finish();
				// also add sender to the list
				solutionsSent.add(worker);
			}            
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
			if (!master.equals(ibis)) {
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
            // recv port for receiving job requests
            requestRecvPort = myIbis.createReceivePort(manyToOneType, 
													"receiveRequestPort");
            requestRecvPort.enableConnections();
	    	// send ports to send jobs to workers
            sendPortsInitialization();
			
			// start time
            long start = System.currentTimeMillis();

            int bound = 0;
            int result = 0;
	    	Cube nextCube = null;
            System.out.print("Bound now:");
	    	
			// each iteration corresponds to one bound
            while (result == 0) {
	    		// increase current bound
            	bound++;
            	initialCube.setBound(bound);
            	System.out.print(" " + bound);
	  			
            	if (bound <= masterBound) {
					// try to solve the cube until masterbound is reached
                	result = solutions(initialCube, cache);
					if (result > 0) {
						// if solution is found send orders to stop waiting
						// so that all ibises end normally
						sendOrders();
						sendSecondOrders(stop);
					}
            	} else {
					//create and send jobs after masterbound is reached
                	createQueue(initialCube, cache);
                	// start working while also checking for requests
                	while (!jobQueue.isEmpty()) {
						nextCube = jobQueue.pollFirst();
						result += solutions(nextCube, cache);
						cache.put(nextCube);
                	}
                	// order workers to report their solutions
                	result += sendOrders();         
			
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
	 * steal work from a random other worker, add it to my queue and work on it
	 * return the number of solutions found in the stolen cubes
	 */
	public int stealWork() throws Exception {
		int solutionsFound = 0;
		Object stolenJob = null;
		Cube nextCube = null;
		
		// get a random other worker
		Random rand = new Random();
		IbisIdentifier victim = null;
		// check that the random identifier is neither the master nor me
		do {
			victim = ibises[rand.nextInt(ibises.length)];
		} while (victim.equals(master) || victim.equals(myIbis.identifier()));
		// send a steal request to the victim
		SendPort stealRequestPort = stealSendPorts.get(victim);	
		WriteMessage stealRequest = stealRequestPort.newMessage();
		stealRequest.writeString(steal);
		stealRequest.finish();	
		
		// try to receive reply to steal request within the timeout limit
		// the limit is set just for the case of blocking due to simultaneous stealing
		try {
			ReadMessage stealReply = stealRecvPort.receive(10);
			stolenJob = stealReply.readObject();
			stealReply.finish();
		// if no worker replies to requests anymore, it means that they 
		// have all emptied their queues so abort stealing
		} catch (ibis.ipl.ReceiveTimedOutException e) {
			return solutionsFound;
		}
		// in case the victim replied
		try {
			//check if the message includes cubes
			Cube[] cubes = (Cube[]) stolenJob;
			if (cubes.length > 0) {
			   	for (Cube cube : cubes) {
					jobQueue.add(cube);
			    }
				// search the received cubes
				while (!jobQueue.isEmpty()) {
					nextCube = jobQueue.pollFirst();
					solutionsFound += solutions(nextCube, cache);
					cache.put(nextCube);
					sendJob();
				}
			//no cubes received
			} else {
				solutionsFound = 0;
			}
		// string received: the victim sent a steal request at the same time 
		} catch (Exception e) {
			solutionsFound = 0;
		}
		return solutionsFound;
	}


	/**
	 * check the orders the master sent, send my solutions to master and then keep
	 * checking for steal requests untill second orders have been received
	 * @param message 
	 * 				the master's report orders
	 * @param solutionsFound 
	 * 				 worker's number of solutions for this bound
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
			// keep checking for steal requests until I 
			// receive second orders (stop or continue working)
			ReadMessage nextMoveMsg = null;
			boolean secondOrdersReceived = false;
			while (!secondOrdersReceived) {
				// check for steal requests
				sendJob();
				// check for second orders
				nextMoveMsg = jobRecvPort.poll();
				if (nextMoveMsg != null) {
					secondOrdersReceived = true;
				}
			}
			String nextMove = nextMoveMsg.readString();
			nextMoveMsg.finish();
			//if next move orders message is stop, stop working
			if (nextMove.equals(stop)) {
				done = true;
			//else continue working
			} else if (!nextMove.equals(cont)) {
				System.err.println("Didn't receive second orders!");
			}
		// wrong orders
		} else {
			System.out.println("Didn't receive first orders!");
		}

		return done;
	}
	
	
	/**
	 * worker's procedure: initialize ports and then send job request to master, receive reply. 
	 * If the reply contains cubes, work on them. If the reply contains an orders string, 
	 * try to steal work, then check the orders (send solutions to master and continue to the 
	 * next bound or stop, depending on the master's call)
	 */
	public void workerProcess() throws Exception {
		
		// recv port for stealing
		stealRecvPort = myIbis.createReceivePort(manyToOneType, "receiveStolenJobPort");
		stealRecvPort.enableConnections();
		// send port to master
		requestSendPort = myIbis.createSendPort(manyToOneType);
		requestSendPort.connect(master, "receiveRequestPort");
		// send ports to other workers for stealing
		sendPortsInitialization();
		// recv port for jobs from master
		jobRecvPort = myIbis.createReceivePort(oneToOneType, "receiveJobPort");
		jobRecvPort.enableConnections();
		
		// start requesting and working

		// flag to check if solution has been found
		boolean done = false;
		int solutionsFound = 0;
		Cube nextCube = null;
		
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
				// add cubes to personal queue
				for (Cube cube : cubes) {
					jobQueue.add(cube);
				}
				// search the cubes for solutions
				// by extracting them one by one from the queue's start
				while (!jobQueue.isEmpty()) {
					nextCube = jobQueue.pollFirst();
					solutionsFound += solutions(nextCube, cache);
					cache.put(nextCube);
					// check for steal requests
					sendJob();
				}	
			// report orders string received instead of cubes
			} catch (ClassCastException exc) {
				// master does not participate in work stealing
				// so we need atleast 2 workers
				if (ibises.length > 2) {
					// try to steal job from another worker
					solutionsFound += stealWork();
				}
				// follow the orders
				done = checkOrders((String) job, solutionsFound);
			}
		}
	}


	public static void printUsage() {
		System.out.println("Rubiks Cube solver");
		System.out.println("");
		System.out
				.println("Does a number of random twists, then solves the rubiks cube with a simple");
		System.out
				.println(" brute-force approach. Can also take a file as input");
		System.out.println("");
		System.out.println("USAGE: Rubiks [OPTIONS]");
		System.out.println("");
		System.out.println("Options:");
		System.out.println("--size SIZE\t\tSize of cube (default: 3)");
		System.out
				.println("--twists TWISTS\t\tNumber of random twists (default: 11)");
		System.out
				.println("--seed SEED\t\tSeed of random generator (default: 0");
		System.out
				.println("--threads THREADS\t\tNumber of threads to use (default: 1, other values not supported by sequential version)");
		System.out.println("");
		System.out
				.println("--file FILE_NAME\t\tLoad cube from given file instead of generating it");
		System.out.println("");
	}

	
	/**
	 * Initializes all ibises and elects a master
	 */
	private void joinAndElectMaster() throws Exception {
		// create ibis
		myIbis = IbisFactory.createIbis(capabilities, null, oneToOneType, manyToOneType);
		// wait until all ibises have joined the pool
		myIbis.registry().waitUntilPoolClosed(); 
		// keep count of the nodes
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
