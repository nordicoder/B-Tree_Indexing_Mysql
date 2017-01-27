/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <cstring>
#define cast reinterpret_cast<char*>
#include <iostream>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
	std::fill(buffer, buffer + 1024, 0); 
	rootPid = -1;
	treeHeight = 0; 
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return res code. 0 if no res
 */
RC BTreeIndex::open(const string& indexname, char mode){
	int temp_pid;
	int t_ht;
	  RC res = pf.open(indexname, mode);
	if(res!=0)
		return res;
	int end_pid = pf.endPid();
	if(end_pid ==0 ){
		treeHeight = 0;
		rootPid = -1;
		RC res = pf.write(0, buffer);
		return res;
	}
	
	res = pf.read(0, buffer);
	if(res!=0)
		return res;
	
	std::copy(buffer, buffer+sizeof(int), (unsigned char *)&temp_pid);
	std::copy(buffer+4, buffer+4+sizeof(int), (unsigned char *)&t_ht);
	
	if(temp_pid > 0 && t_ht >= 0){
		treeHeight = t_ht;
		rootPid = temp_pid;
	}
	return 0;
}

/*
 * Close the index file.
 * @return res code. 0 if no res
 */
RC BTreeIndex::close(){
	std::copy(cast(&rootPid), cast(&rootPid)+sizeof(int), buffer);
	std::copy(cast(&treeHeight), cast(&treeHeight)+sizeof(int), buffer+4);
	RC res = pf.write(0, buffer);
	if(res != 0)
		return res;
	res = pf.close();
    	return res;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return res code. 0 if no res
 */
RC BTreeIndex::insert(int key, const RecordId& rid){

	if(key < 0)
		return RC_INVALID_ATTRIBUTE;
	RC res;
	BTLeafNode n_tree;
	int i_key = -1;
	PageId i_pid = -1;
	if(treeHeight == 0){
		n_tree.insert(key, rid);
		int end_pid = pf.endPid();
		if(end_pid == 0)
			rootPid = 1;
		else
			rootPid = end_pid;
		
		treeHeight++;
		
		return n_tree.write(rootPid, pf);
	}
	res = insertHelper(key, rid, 1, rootPid, i_key, i_pid);
	return res;
}

//Recursive function for inserting key into correct leaf and non-leaf nodes alike
RC BTreeIndex::insertHelper(int key, const RecordId& rid, int currHeight, PageId thisPid, int& t_key, PageId& temp_pid){

	t_key = -1;
	temp_pid = -1;
	BTLeafNode my_leaf, next_leaf;
	BTNonLeafNode n_root, middle_node, next_middle_node;
	PageId c_pid = -1;
	int a_key, i_key = -1;
	PageId i_pid = -1;
	RC res;
	if(currHeight == treeHeight){
		my_leaf.read(thisPid, pf);
		if(my_leaf.insert(key, rid)==0){
			my_leaf.write(thisPid, pf);
			return 0;
		}
		res = my_leaf.insertAndSplit(key, rid, next_leaf, a_key);
		if(res != 0)
			return res;
		int lastPid = pf.endPid();
		temp_pid = lastPid;
		t_key = a_key;
		next_leaf.setNextNodePtr(my_leaf.getNextNodePtr());
		my_leaf.setNextNodePtr(lastPid);
		res = next_leaf.write(lastPid, pf);
		if(res != 0)
			return res;
		res = my_leaf.write(thisPid, pf);
		if(res != 0)
			return res;
		if(treeHeight == 1){
			treeHeight++;
			n_root.initializeRoot(thisPid, a_key, lastPid);
			rootPid = pf.endPid();
			n_root.write(rootPid, pf);
		}
		return 0;
	}
	else{
		middle_node.read(thisPid, pf);
		middle_node.locateChildPtr(key, c_pid);
		res = insertHelper(key, rid, currHeight+1, c_pid, i_key, i_pid);
		if(!(i_key == -1 && i_pid == -1)){ 
			if(middle_node.insert(i_key, i_pid) == 0){
				middle_node.write(thisPid, pf);
				return 0;
			}
			middle_node.insertAndSplit(i_key, i_pid, next_middle_node, a_key);
			int lastPid = pf.endPid();
			t_key = a_key;
			temp_pid = lastPid;
			res = middle_node.write(thisPid, pf);
			if(res != 0)
				return res;
			res = next_middle_node.write(lastPid, pf);
			if(res != 0)
				return res;
			if(treeHeight==1){
				treeHeight++;
				n_root.initializeRoot(thisPid, a_key, lastPid);
				rootPid = pf.endPid();
				n_root.write(rootPid, pf);
			}
			
		}
		return 0;
	}
}

/*
 * Find the leaf-node index entry whose key value is larger than or 
 * equal to searchKey, and output the location of the entry in IndexCursor.
 * IndexCursor is a "pointer" to a B+tree leaf-node entry consisting of
 * the PageId of the node and the SlotID of the index entry.
 * Note that, for range queries, we need to scan the B+tree leaf nodes.
 * For example, if the query is "key > 1000", we should scan the leaf
 * nodes starting with the key value 1000. For this reason,
 * it is better to return the location of the leaf node entry 
 * for a given searchKey, instead of returning the RecordId
 * associated with the searchKey directly.
 * Once the location of the index entry is identified and returned 
 * from this function, you should call readForward() to retrieve the
 * actual (key, rid) pair from the index.
 * @param key[IN] the key to find.
 * @param cursor[OUT] the cursor pointing to the first index entry
 *                    with the key value.
 * @return res code. 0 if no res.
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor){
	BTLeafNode leaf;
	int eid, currHeight = 1;
	RC res;	
	BTNonLeafNode middle_node;
	PageId nextPid = rootPid;
	
	while(currHeight != treeHeight){
		res = middle_node.read(nextPid, pf);
		if(res!=0)
			return res;
		res = middle_node.locateChildPtr(searchKey, nextPid);
		if(res!=0)
			return res;
		currHeight++;
	}
	res = leaf.read(nextPid, pf);
	if(res!=0)
		return res;
	res = leaf.locate(searchKey, eid);
	if(res!=0)
		return res;
	cursor.eid = eid;
	cursor.pid = nextPid;
	return res;
}

//Recursive function for determining location where a search key belongs
//Runs until we hit the base case of finding the search key's corresponding leaf node
RC BTreeIndex::locateRec(int searchKey, IndexCursor& cursor, int currHeight, PageId& nextPid){
	RC res;
	int eid = -1;
	BTLeafNode leaf;
	BTNonLeafNode middle_node;
	if(searchKey<0)
		return RC_INVALID_ATTRIBUTE;
		
	if(currHeight == treeHeight){ 
		res = leaf.locate(searchKey, eid);
		if(res != 0)
			return res;
		res = leaf.read(nextPid, pf);
		if(res != 0)
			return res;
		cursor.pid = nextPid;
		cursor.eid = eid;
		return res;
	}
	res = middle_node.read(nextPid, pf);
	if(res != 0)
		return res;
	res = middle_node.locateChildPtr(searchKey, nextPid);
	if(res != 0)
		return res;
	res = locateRec(searchKey, cursor, currHeight-1, nextPid);
	return res;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return res code. 0 if no res
 */ 
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid){
	PageId cursorPid = cursor.pid;
	RC res;
	int cursorEid = cursor.eid;
	BTLeafNode leaf;
	res = leaf.read(cursorPid, pf);
	if(res != 0)
		return res;
	res = leaf.readEntry(cursorEid, key, rid);
	if(res != 0)
		return res;
	if(cursorPid <= 0)
		return RC_INVALID_CURSOR;
	if(cursorEid+1 >= leaf.getKeyCount()){
		cursorEid = 0;
		cursorPid = leaf.getNextNodePtr();
	}
	else
		cursorEid++;
	cursor.pid = cursorPid;
	cursor.eid = cursorEid;
	return 0;
}

PageId BTreeIndex::getRootPid(){
	return rootPid;
}

int BTreeIndex::getTreeHeight(){
	return treeHeight;
}

