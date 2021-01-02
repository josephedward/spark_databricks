Arbitrary Stateful Processing 

The first section if this chapter demonstrates how Spark maintains information and updates windows based on our specifications.
-  But things differ when you have more complex concepts of windows; this is, where arbitrary stateful processing comes in. 

This section includes several examples of different use cases along with examples that show you how you might go about setting up your business logic. Stateful processing is available only in Scala in Spark 2.2. This will likely change in the future. 

When performing stateful processing, you might want to do the following: 
- Create window based on counts of a given key 
- Emit an alert if there is a number of events within a certain time frame 
- Maintain user sessions of an undetermined amount of time and save those sessions to perform some analysis on later. 

At the end of the day, there are two things you will want to do when performing this style of processing: 
- Map over groups in your data, operate on each group of data, and generate at most a single row for each group. The relevant API for this use case is mapGroupsWithState. 
- Map over groups in your data, operate on each group of data, and generate one or more rows for each group. The relevant API for this use case is flatMapGroupsWithState. 

When we say “operate” on each group of data, that means that you can arbitrarily update each group independent of any other group of data. This means that you can define arbitrary window types that don’t conform to tumbling or sliding windows like we saw previously in the chapter.

One important benefit that we get when we perform this style of processing is control over configuring time-outs on state. 
- With windows and watermarks, it’s very simple: you simply time-out a window when the watermark passes the window start. 
- This doesn’t apply to arbitrary stateful processing, because you manage the state based on user-defined concepts.
-  Therefore, you need to properly time-out your state. 

Time-Outs 

As mentioned in Chapter 21, a time-out specifies how long you should wait before timing-out some intermediate state.
-  A time-out is a global parameter across all groups that is configured on a per-group basis. 
- Time-outs can be either based on processing time (GroupStateTimeout.ProcessingTimeTimeout) or event time (GroupStateTimeout.EventTimeTimeout). 

When using time-outs, check for time-out first before processing the values. 
- You can get this information by checking the state.hasTimedOut flag or checking whether the values iterator is empty. 
- You need to set some state (i.e., state must be defined, not removed) for time-outs to be set. 
- With a time-out based on processing time, you can set the time-out duration by calling GroupState.setTimeoutDuration (we’ll see code examples of this later in this section of the chapter). 
- The time-out will occur when the clock has advanced by the set duration. Guarantees provided by this time-out with a duration of D ms are as follows: - 
	- Time-out will never occur before the clock time has advanced by D ms 
	- Time-out will occur eventually when there is a trigger in the query (i.e., after D ms).So there is a no strict upper bound on when the time-out would occur. For example, the trigger interval of the query will affect when the time-out actually occurs. If there is no data in the stream (for any group) for a while, there won’t be any trigger and the timeout function call will not occur until there is data. 

Because the processing time time-out is based on the clock time, it is affected by the variations in the system clock. This means that time zone changes and clock skew are important variables to consider. 

With a time-out based on event time, the user also must specify the event-time watermark in the query using watermarks. When set, data older than the watermark is filtered out. As the developer, you can set the timestamp that the watermark should reference by setting a time-out timestamp using the GroupState.setTimeoutTimestamp(...) API. The time-out would occur when the watermark advances beyond the set timestamp. Naturally, you can control the time-out delay by either specifying longer watermarks or simply updating the time-out as you process your stream. Because you can do this in arbitrary code, you can do it on a per-group basis. The guarantee provided by this time-out is that it will never occur before the watermark has exceeded the set time-out. 

Similar to processing-time time-outs, there is a no strict upper bound on the delay when the timeout actually occurs. The watermark can advance only when there is data in the stream, and the event time of the data has actually advanced. 

NOTE
-  Although time-outs are important, they might not always function as you expect. For instance, as of this writing, Structured Streaming does not have asynchronous job execution, which means that Spark will not output data (or time-out data) between the time that a epoch finishes and the next one starts, because it is not processing any data at that time.
-  Also, if a processing batch of data has no records (keep in mind this is a batch, not a group), there are no updates and there cannot be an event-time time-out. This might change in future versions. 

Output Modes 
One last “gotcha” when working with this sort of arbitrary stateful processing is the fact that not all output modes discussed in Chapter 21 are supported. This is sure to change as Spark continues to change, but, as of this writing
- mapGroupsWithState supports only the update output mode,
- flatMapGroupsWithState supports append and update. 
	- append mode means that only after the time-out (meaning the watermark has passed) will data show up in the result set. 
	- This does not happen automatically, it is your responsibility to output the proper row or rows. 
	- Please see Table 21-1 to see which output modes can be used when. 

mapGroupsWithState 
- This is similar to a user-defined aggregation function that takes as input an update set of data and then resolves it down to a specific key with a set of values. 
- There are several things you’re going to need to define along the way: 
	- Three class definitions: 
		- an input definition, 
		- a state definition, and 
		- optionally an output definition. 
	- A function to update the state based on a key, an iterator of events, and a previous state. 
	- A time-out parameter (as described in the time-outs section). 
- With these objects and definitions, you can control arbitrary state by creating it, updating it over time, and removing it. 

Let’s begin with an example of simply updating the key based on a certain amount of state, and then move onto more complex things like sessionization. Because we’re working with sensor data, let’s find the first and last timestamp that a given user performed one of the activities in the dataset. This means that the key we will be grouping on (and mapping on) is a user and activity combination. NOTE When you use mapGroupsWithState, the output of the dream will contain only one row per key (or group) at all times. If you would like each group to have multiple outputs, you should use flatMapGroupsWithState (covered shortly). Let’s establish the input, state, and output definitions:

```
case class InputRow(user:String, timestamp:java.sql.Timestamp, activity:String)
case class UserState(user:String,
var activity:String,
var start:java.sql.Timestamp,
var end:java.sql.Timestamp)
```

set up the function that defines how you will update your state based on a given row

```
def updateUserStateWithEvent(state:UserState, input:InputRow):UserState = {
if (Option(input.timestamp).isEmpty) {
return state
}
if (state.activity == input.activity) {
if (input.timestamp.after(state.end)) {
state.end = input.timestamp
}
if (input.timestamp.before(state.start)) {
state.start = input.timestamp
}
} else {
if (input.timestamp.after(state.end)) {
state.start = input.timestamp
state.end = input.timestamp
state.activity = input.activity
}
}
state
}
```