function(doc, site) {
  var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
  // Can't get multiple keys to work - use one for now
  //emit([ele["Status"], ele["RequestName"], doc._id, doc.timestamp, ele["ParentQueueId"]],
    emit(ele["RequestName"], {'_id' : doc['_id']});
}