# ABOUT PERCENTILE BASED HISTOGRAM BIN GENERATION
Author: Kagan Turgut  kagan@brane.com
(c) all rights reserved

## The goal of this algorithm is to generate histogram bins from percentile values:
This algorithm generates bins that can be used in histogram feature generation for machine learning models, analyzing data 
characteristics such as sparseness, skew, bin coverage, and noise.
Algorithm takes these input parameters:
- **percentileValues** : x-% percentile values of a column by a sampling ratio (candidate: 2.5%)
Note:We use percentile values only as input for performance reason
e.g. [1,,,2,,,3,3,,,3,,,,10,1000,,,,1000,11000] for x=0,2.5,5.0,,,,97.5,100
note: 20% of values are 3, 20% of values are 1000,
- **maxNumberOfBins**: maximum number of bins that we want to generate. This also is used as the minimum threshold for a bin
to be generated as an equality bin, instead of a range bin. Equality bins are denoted as [value,value], versus range bins 
are denoted as [lowerValue,UpperValue), where '[,']' represents inclusivity, vs '(',')' exclusivity. For example an equality bin
[3,3] implies value is exactly equal to 3, versus, a range bin : [3,5) implies  3 >= value > 5
- **minSupport**: each bin represent a frequency of values, in other words, percent of data coverage. In order to improve the 
feature generation execution performance, we drop bins below a certain frequency identified as "minSupport" from the result.
For example if our candidates are: [3:5):2, [5,8):4, [8:8]:4, (8,10]:1, and minSupport is 10%, the last bin which has less
than 10% of the data coverage will be dropped.

## Concepts:
- **Candidate Bins**: Candidate bins are the initial bins created from the percentile values, and the high frequency values
- **RangeThresholdCandidates**: ordered set of distinct range sizes for all bins
- **nTopDensityDifference**: Index of ordered top range sizes collected from all bins
- **PartitionPlan**: PartitionPlan represents instructions as to how the candidate bins will be merged / split / dropped based on
various policies and constraints. PartitionPlan helps separate actual manipulation of the bins from the various policies applied. 
This simplifies implementation, testing; and also allows us to evolve policy decisions with ease. 
Partition plan has a signal in terms of an integer array, where each entry corresponds to a bin in candidate bins at same index. 
Each integer value has a special meaning: 
- 1 for Equality bin, 
- 0 for High density bin,  
- negative nTopDensityDifference for Low density bin. 
- 2 for slicing done during breaking of the largest logical bin groups by size. 
- e.g. [1,0,0,-2,-1] is a compact representation to mean: 1 equality bean followed by two high density range beans, 
followed by 2 low density range beans. Since -1 corresponds to nTopDensityDifference=1, the last bin meets  the highest 
range threshold, then the bin indicated by -2.
- ** Logical Bin Group** bins are logically grouped into two categories which help determine how they will be sliced into sub groups:
  **Uniform** data distribution: 2 or more consecutive bins that are not low density or equality bin.
  **Nonlinear** data distribution: 2 or more consecutive bins with generated with increasing negative nTopDensityDifference count are designated as low density bins.  
  e.g. [1,0,0,0,0,0,1,0,0,1] => Implies two uniform data distribution groups, largest marked in bold, with size of bin group is 5
  e.g. [0,0,-4,-3,0,0,0,-4,-3,-2,-1] => implies two nonlinear data distribution bin groups, the largest one series designated in bold, with size of bing group is 4
     

## Objectives
A : The number of output bins is less than **maxNumberOfBins** : default = 7
B : Create equality bins for values with frequency above a threshold : default = 1/**maxNumberOfBins**   
B : Range size of output bins are smaller than rangeThreshold:  default = (1 / **maxNumberOfBins**) * 0.5
C : Range frequencies of resulting bins are as even as possible
D : Coverage of the resulting bins are not below coverageThreshold: default = (10 / **maxNumberOfBins**) * 6

## Approach:
Bin partitions will are chosen applying the following policies in order:
   1- for high frequency values in data above a certain threshold are created as equality bins. These high frequency values 
   are represented as repeating percentile values in actual data since algorithm assumes a high frequent value to have at 
   least "1/maxNumberOfBeans" frequency. 
   2- detecting the density changes in the percentile values. density measured as difference between max and min value. 
   data is first partitioned along the maximum threshold. 
   3- detecting the distribution of data (uniform, non-uniform such as exponential) which form a bin group, and slicing these    
bins groups evenly, each time increasing the expected output number of bins one at a time.
   - Note algorithm uses top-down approach in slicing since it yields an even distribution of bin sizes at all times.
   4- removing the bins that are below minSupport threshold from the result set.

## Algorithm
Step 1: Count ratio frequency of each distinct value in percentileValues and extract values which frequency is larger than 100%/(maxNumberOfBins) 
Step 2: Generate equalityBins for each high frequent values found in 1 and create a range bin for each value in percentileValues
        e.g. [1,2),[2,3),[3,3],(3,4),[4,5),,,[10,1000),[1000,1000], (1000,110001]
Step 3: Create candidate bins. Compute frequency (number of values in percentileValues falling into a bin) and range size (upperbound - lowerbound of a bin) 
        for each bin in 2
Step 4: Evaluate PartitionPlans by density. Generate a series of PartitionPlans increasing the nTopDensityDifference from 1 to size of RangeThresholdCandidates 
Step 5: If exit criteria not met, Evaluate a series of PartitionPlans by frequency (ie. size). Each time increasing the expected number of output bins by 1, 
        always evenly redistributing the existing bins.
      - Note that during partitioning by size, the largest non-linear bin group signal is masked into 0's for bins to be merged and 2s for where they will 
      be split as follows: Given initial plan with signal [0,0,-4,-3,0,0,0,-4,-3,-2,-1] => the biggest group is [-4,-3,-2,-1] => [0,0,-4,-3,0,0,0,2,0,2,0]
Step 6: Dropping the bins that represent low density (i.e. sparse ranges) <- actual dropping not yet fully implemented
      - If there is a consecutive set of bins with non-linear increase in values, Only the very first one with the lowest nTopDensityDifference is to be dropped
      - e.g. [0,0,-4,-3,0,0,0,-4,-3,-2,-1] => Only the last candidate bin with [-1] is to be dropped.
      
## Next Steps
Incorporate this with the Spark's approximate percentile calculation function and pass the percentile values on a sample dataFrame grouping columns in groups 