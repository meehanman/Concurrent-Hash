/*===========================================================
 * (C) Hans Vandierendonck, 2013-15
 *===========================================================*/
import java.util.concurrent.*;
import java.util.Random;
import java.util.HashSet;

/*===========================================================
 * Auxiliary class to perform timing measurement and control
 * the experiments.
 *===========================================================*/
class ResultAggregator  {
    private int num_rounds;
    private int num_contains;
    private int num_threads;
    private volatile int round;
    private final CyclicBarrier barrier;
    private long sum_rate;
    private long sum_rate_sq;
    private double sum_delay;
    private long last_time;
    private Contents contents;
    private long num_values[];
    private volatile boolean round_done;

    ResultAggregator( int nr, int nc, int np, Contents c ) {
	num_rounds = nr;
	num_contains = nc;
	num_threads = np;
	round = 0;
	sum_rate = sum_rate_sq = 0;
	sum_delay = 0;
	last_time = System.nanoTime();
	contents = c;

	num_values = new long[np];
	for( int i=0; i < np; ++i )
	    num_values[i] = 0;

	round_done = false;

	// np+1: main thread also joins barrier to synchronise timer
	barrier = new CyclicBarrier( np+1,
				     new Runnable() {
					 public void run() {
					     long now = System.nanoTime();
					     long delay = now - last_time;
					     double f_delay = (double)delay * 1e-9;
					     long nv = 0; 
					     for( int i=0; i < num_threads; ++i ) {
						 nv += num_values[i];
						 num_values[i] = 0;
					     }

					     if( round > 0 ) { // skip 1st round
						 double r = (double)nv / f_delay;
						 sum_rate += r;
						 sum_rate_sq += r*r;
						 sum_delay += f_delay;
					     }
					     round++;
					     if( round > 1 ) {
						 System.out.println( "Delay is " + f_delay + " secs" + " operations is " + nv );
					     } else {
						 System.out.println( "delay is " + f_delay + " secs (warmup)" );
					     }
					     contents.nextRound();
					     round_done = false;
					     System.gc();
					     System.gc();
					     System.gc();
					     last_time = System.nanoTime();
					 }
				     } );
    }

    public boolean isFinished() {
	return round >= num_rounds;
    }

    public boolean isRoundDone() {
	return round_done;
    }

    public void setRoundDone() {
	round_done = true;
    }

    public double getAvgDelay() {
	return (double)sum_delay / (double)(num_rounds-1);
    }

    public double getAvgRate() {
	return (double)sum_rate / (double)(num_rounds-1);
    }

    public double getStdDevRate() {
	double s1 = (double)sum_rate;
	double s2 = (double)sum_rate_sq;
	double N = (double)(num_rounds-1);
	return Math.sqrt( (N*s2-s1*s1)/(N*(N-1)) );
    }

    public void syncBarrier( int tid, long nv ) {
	if( tid >= 0 )
	    num_values[tid] = nv;
	try {
	    barrier.await();
	} catch( InterruptedException ex ) {
	} catch( BrokenBarrierException ex ) {
	}
    }
}

/*===========================================================
 * Auxiliary class to hold a value
 *===========================================================*/
class IntValue {
    int v;

    IntValue( int vv ) { v = vv; }

    @Override
    public int hashCode() {
	return (v >> 16) ^ v;
    }

    @Override
    public boolean equals( Object obj ) {
	if( !(obj instanceof IntValue) )
	    return false;
	if( obj == this )
	    return true;

	IntValue rhs = (IntValue)obj;
	return v == rhs.v;
    }
}

/*===========================================================
 * Contents generation
 *===========================================================*/
// Auxiliary class to generate a repeating sequence of values
// in a predictable and repeatable manner, without duplicates.
// A repeatable series of distinct values in the range 1..PERIOD
// (inclusive boundaries) is generated.
class LFSR {
    public static final int BITS = 19;
    public static final int RANGE = 1<<BITS;
    public static final int PERIOD = RANGE-1;

    private int lfsr = 1; // 0 is not included in the sequence!

    public int current() {
	return lfsr;
    }

    public int next() {
	// x^19 + x^18 + x^17 + x^14 + 1
	int bit = ((lfsr >> 0) ^ (lfsr >> 1) ^ (lfsr >> 2) ^ (lfsr >> 5) ) & 1;
	lfsr =  (lfsr >> 1) | (bit << (BITS-1));
	return lfsr;
    }

    // Brute force - there probably is a smarter way to do this.
    public void fastforward(int steps) {
	for( int s=0; s < steps; ++s )
	    next();
    }

    public void syncstate( LFSR l ) {
	lfsr = l.lfsr;
    }
};

class Contents {
    private int num_elems;
    private int nthreads;
    private int elems_per_thread;
    private int values[];
    private Hash<IntValue,IntValue> hashtable;
    private LFSR generator[];
    public static final int MAX_LOG_THREADS = 6;
    public static final int MAX_THREADS = 1 << MAX_LOG_THREADS;
    public static final int USED_RANGE = LFSR.RANGE << MAX_LOG_THREADS;
    public static final int NEVER_PRESENT = USED_RANGE+1;
    public static final int FIRST_UNREPLACEABLE = USED_RANGE+2;

    Contents( int ne, int nt ) {
	num_elems = ne;
	nthreads = nt;
	elems_per_thread = num_elems / nthreads;
	values = new int[num_elems];
	generator = new LFSR[nthreads];
	for( int i=0; i < nthreads; ++i )
	    generator[i] = new LFSR();
    }

    void nextRound() {
	// Remaining hook from previous code version.
    }

    // Second line of defense: further diversify values by inserting thread
    // id. Insert the id in the low-order bits to encourage concurrent updates
    // on neighbouring values in the chains.
    // This is a temporary fix. We are really looking for generators with
    // polynomials that have a period divisable by the number of threads.
    public int mash_thread(int val, int tid) {
	return (val << MAX_LOG_THREADS) | tid;
    }

    void initialize( Hash<IntValue,IntValue> hash ) {
	hashtable = hash;

	// Generate initial contents for the hashtable using one of the
	// generators.
	// The number of elements in the hash table must be a multiple
	// of the number of threads, otherwise the driver will not be able
	// correctly anticipate what elements are in the hash table.
	// Slightly modify the hash table size such that it is a multiple
	// of the number of threads.
	int replaceable = num_elems - num_elems % nthreads;
	for( int i=0; i < replaceable; ++i ) {
	    // int pos = (no %  elems_per_thread) + tid * elems_per_thread;
	    values[i] = mash_thread(generator[0].next(), i / elems_per_thread);
	    hash.add( new IntValue(values[i]), new IntValue(0) );
	}
	for( int i=replaceable; i < num_elems; ++i ) {
	    values[i] = mash_thread(FIRST_UNREPLACEABLE + i - replaceable, 0);
	    hash.add( new IntValue(values[i]), new IntValue(0) );
	}

	// Setup generators with a shift such that all threads together
	// generate a set of unique values. The shift is by 1. Each thread
	// will thus step over nthreads-1 values after generating one value.
	for( int i=1; i < nthreads; ++i ) {
	    generator[i].syncstate( generator[0] );
	    generator[i].fastforward(i);
	}

	nextRound();
    }

    int getsl( int k ) {
	// A likely successful lookup - may fail because it may conflict with
	// an add (pending) or a remove (completed) of this element by another
	// thread.
	return values[k];
    }
    int getad( int no, int tid ) {
	return mash_thread( generator[tid].next(), tid ); // replacement
    }
    int getrm( int no, int tid ) {
	// A successful remove. The set of values is distributed between
	// threads, each thread adding/removing on a subset of the values
	// array. This way, data races on the values[] array are avoided.
	// We expect nonetheless interference in the hash table as the
	// hash table hashes in a different way than the pseudo-random
	// numbers are generated and distributed between threads.
	int pos = (no %  elems_per_thread) + tid * elems_per_thread;
	// System.out.println( "pos=" + pos + " k=" + k + " tid=" + tid );
	int ret = values[pos];
	values[pos] = mash_thread( generator[tid].current(), tid ); // replacement (add)
	// Each thread must step over the values generated by the other threads.
	generator[tid].fastforward(nthreads-1);
	return ret;
    }
};

/*===========================================================
 * Process definition for processes that will generate
 * accesses to the hash table.
 *===========================================================*/
class TestProcess extends Thread {
    private Hash<IntValue,IntValue> hashtable;
    private int num_elems;
    private int num_contains;
    private int tid;
    private ResultAggregator agg;
    private Contents contents;

    TestProcess( int num_elems_, int num_contains_, int tid_,
		 Hash<IntValue,IntValue> hash, ResultAggregator agg_,
		 Contents cnt_ ) {
	hashtable = hash;
	num_contains = num_contains_;
	num_elems = num_elems_;
	tid = tid_;
	agg = agg_;
	contents = cnt_;
    }

    public void run() {
	final Random rng = new Random();
	int idx = 0;
	while( !agg.isFinished() ) {
	    long v = 0;
	    while( !agg.isRoundDone() ) {
		// Lookups. Do a series of contains per
		// add/remove pair to reflect typical usage.
		for( int l=0; l < num_contains && !agg.isRoundDone(); ++l, ++v ) {
		    int kg = rng.nextInt(num_elems+(num_elems>>3));
		    if( kg >= num_elems ) {
			// An unsuccessful lookup (~10% of all contains)
			int k = Contents.NEVER_PRESENT;
			hashtable.get( new IntValue(k) );
		    } else {
			// A successful lookup
			int vg = contents.getsl( kg );
			hashtable.get( new IntValue(vg) );
		    }
		}

		// A remove and add operation.
		int va = contents.getad( idx, tid );
		int vr = contents.getrm( idx, tid );

		// System.out.println( "lookup: " + vg + " remove: " + vr + " add:" + va + " tid=" + tid );

		// A (pseudo-)random insertion
		if( !hashtable.add( new IntValue(va), new IntValue(1) ) )
		    System.err.println( "Add failed: " + vr + " tid=" + tid );

		// A (pseudo-)random deletion
		if( !hashtable.remove( new IntValue(vr) ) )
		    System.err.println( "Remove failed: " + vr + " tid=" + tid );
		// Another set of values added
		v += 2; // add and del
		idx++; // next element to replace
	    }
	    agg.syncBarrier( tid, v );
	}
    }
}

/*===========================================================
 * Process definition for a process that will generate
 * resize actions on the hash table.
 *===========================================================*/
class ResizeProcess extends Thread {
    private Hash<IntValue,IntValue> hashtable;
    private ResultAggregator agg;
    private long num;

    ResizeProcess( Hash<IntValue,IntValue> hash, ResultAggregator agg_ ) {
	hashtable = hash;
	agg = agg_;
	num = 0;
    }

    public void run() {
	int sizes[] = new int[2];
	sizes[0] = hashtable.getArraySize();
	sizes[1] = 2*sizes[0];
	int cur = 1;

	while( !agg.isFinished() ) {
	    hashtable.resize( sizes[cur] );
	    cur = 1 - cur;
	    ++num;
	}
    }

    public long getResizes() {
	return num;
    }
}

/*===========================================================
 * Main FixedTimeDriver Class (main program)
 *===========================================================*/
class FixedTimeDriver {
    public static int parse_integer( String arg, int pos ) {
	int i = 0;
	try {
	    i = Integer.parseInt( arg );
	} catch( NumberFormatException e ) {
	    System.err.println( "Argument " + pos + "'" + arg
				+ "' must be an integer" );
	}
	return i;
    }

    public static double parse_double( String arg, int pos ) {
	double i = 0;
	try {
	    i = Double.parseDouble( arg );
	} catch( NumberFormatException e ) {
	    System.err.println( "Argument " + pos + "'" + arg
				+ "' must be a double" );
	}
	return i;
    }

    public static void main (String[] args) {
	if( args.length < 5 ) {
	    System.err.println("Usage: FixedTimeDriver <num_threads> <init-size> <contains> <measure-elms> <rounds> <resize>");
	    System.exit( 1 );
	}

	int nthreads = parse_integer( args[0], 1 );
	int init_size = parse_integer( args[1], 2 );
	int num_contains = parse_integer( args[2], 3 );
	int msecs_measured = parse_integer( args[3], 4 );
	int num_rounds = parse_integer( args[4], 5 );
	boolean with_resize = parse_integer( args[5], 6 ) != 0;

	if( nthreads > init_size ) {
	    // Need this. The reason is that contents holds an unsynchronized
	    // shared array (if we synchronize it we loose concurrency) and
	    // each thread is given a slice in this array. Calculation of the
	    // address in the slice depends on this property.
	    System.err.println( "Hashtable must have at least as many elements as there are threads (" + nthreads + ")." );
	    System.exit( 1 );
	}

	if( nthreads > Contents.MAX_THREADS ) {
	    System.err.println( "Maximum number of supported threads is "
				+ Contents.MAX_THREADS );
	    System.exit( 1 );
	}

        System.out.println("Measuring performance with " + nthreads
			   + " threads."
			   + " Doing " + num_rounds
			   + " rounds of the experiment after a warmup"
			   + " round during " + msecs_measured
			   + " milliseconds per round.");
        System.out.println("Hashtable contains " + init_size + " elements" );

	Contents contents = new Contents( init_size, nthreads );
	ResultAggregator agg
	    = new ResultAggregator( num_rounds, num_contains, nthreads,
				    contents );

	TestProcess[] processes = new TestProcess[nthreads];

	Hash<IntValue,IntValue> hashtable = new Hash<IntValue,IntValue>(15);

	// Initialize the hash table with values to start non-empty
	contents.initialize( hashtable );

	// Create all of the threads
	for( int i=0; i < nthreads; ++i ) {
	    processes[i]
		= new TestProcess( init_size, num_contains, i,
				   hashtable, agg, contents );
	}
	ResizeProcess resizer = null;
	if( with_resize )
	    resizer = new ResizeProcess( hashtable, agg );

	// Start all of the threads and let them warmup.
	// Warming up is important to let the JIT do it's work (i.e.,
	// compile and optimize the code to make ti faster). When the JIT
	// kicks in, measured performance numbers are distorted and unreliable.
	// The garbage collector is another source of disruption to performance.
	// That's why will call the GC explicitly when reaching the barrier
	// (see barrier creation). You would normally never call the GC
	// directly.
	for( int i=0; i < nthreads; ++i ) {
	    processes[i].start();
	}
	if( with_resize )
	    resizer.start();

	// Trigger timer to terminate every round
	for( int r=0; r < num_rounds; ++r ) {
	    try {
		Thread.sleep( msecs_measured );
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }
	    agg.setRoundDone();
	    agg.syncBarrier( -1, 0 );
	}

	// Join threads (cleanup properly).
	for( int i=0; i < nthreads; ++i ) {
	    try { processes[i].join(); } catch( InterruptedException e ) { }
	}
	if( with_resize ) {
	    try { resizer.join(); } catch( InterruptedException e ) { }
	    System.out.println( "Resizes performed: " + resizer.getResizes() );
	}

	System.out.println( "Hashtable size is: " + hashtable.size() );

	// Get the results out.
	double avg_delay = agg.getAvgDelay();
	double avg_rate = agg.getAvgRate();
	double sdv_rate = agg.getStdDevRate();

	System.out.println( "Average delay of a trial was " + avg_delay
			    + " seconds." );
	System.out.println( "Average operations/sec: " + avg_rate );
	System.out.println( "Standard deviation operations/sec: " + sdv_rate );
    }
}
