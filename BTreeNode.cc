#include "BTreeNode.h"
#include <iostream>
#include <cstring>
#include <stdlib.h>
using namespace std;
#define cast reinterpret_cast<char*>

BTLeafNode::BTLeafNode()
{
	numKeys=0;
	std::fill(buffer, buffer + 1024, 0); //clear the buffer if necessary
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	RC RC_read;
	RC_read= pf.read(pid, buffer);	
	return RC_read;	
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{
	RC RC_write;
	RC_write = pf.write(pid, buffer);
	return RC_write;
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
	
	int record_key_size = sizeof(RecordId) + sizeof(int);
	char *temp =buffer;
	
	int i=0;
		int count=0;

	while(i+record_key_size<=1020 )
	
	{					int current_key=0;

		    //int *ptr=temp;//anything



		std::copy(temp,temp+4,(unsigned char*)&current_key);
		
	//	int header = *reinterpret_cast<const int*>(&buffer[4+i]);
		//cout<<"he"<<header<<"--"<<current_key<<"\n";
		//std::copy(&current_key,&current_key +sizeof(int),temp);
		//cout<<"-"<<sizeof(int);
		if(current_key==0) 
			break;
		count++;
		
		temp += record_key_size; 
		i+=record_key_size;
	}
	
	return count;
}
/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
	int record_key_size = sizeof(RecordId) + sizeof(int);
	PageId neighbour_node = getNextNodePtr();
	int max_num_keys = (1024-sizeof(PageId))/record_key_size;
	if(getKeyCount()+1 > max_num_keys) 
	{
		return RC_NODE_FULL;
	}
	
	char* temp = buffer;
	
	
	int i=0;
		while(i+record_key_size<=1020 )
	{
		int current_key;
				
	//	int header = *reinterpret_cast<const int*>(&buffer[4+i]);
		//cout<<"he"<<header<<"--"<<current_key<<"\n";
		//std::copy(&current_key,&current_key +sizeof(int),temp);
		//cout<<"-"<<sizeof(int);
		
		std::copy(temp,temp+4,(unsigned char*)&current_key);
		
		if(current_key==0 || !(key > current_key))
			break;
		
		temp += record_key_size; 
		i+=record_key_size;
	}
	
	char* new_buff = (char*)malloc(1024);
	std::fill(new_buff, new_buff + 1024, 0); 

	std::copy(buffer,buffer+i,new_buff);
	PageId pid = rid.pid;
	int sid = rid.sid;
	
	std::copy(cast(&key),cast(&key) + sizeof(int),new_buff+i);
	
	memcpy(new_buff+i+sizeof(int), &rid, sizeof(RecordId));
	
	
	std::copy(buffer+i,buffer+i+ getKeyCount()*record_key_size - i,new_buff+i+record_key_size);
	std::copy(cast(&neighbour_node),cast(&neighbour_node)+sizeof(int),new_buff+1024-sizeof(PageId));

	std::copy(new_buff,new_buff+1024,buffer);
	free(new_buff);
	
	numKeys++;
	
	return 0;
}
/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{
		PageId neighbour_node = getNextNodePtr();
	
	int record_key_size = sizeof(RecordId) + sizeof(int);
	int max_num_pairs = (1024-sizeof(PageId))/record_key_size;
	
	if(!(getKeyCount() >= max_num_pairs))
		return RC_INVALID_FILE_FORMAT;
	
	if(sibling.getKeyCount()!=0)
		return RC_INVALID_ATTRIBUTE;
	
	std::fill(sibling.buffer, sibling.buffer + 1024, 0); 
	
	int count_half_keys = ((int)((getKeyCount()+1)/2));
	
	int first_half_index = count_half_keys*record_key_size;
	
	
	
	std::copy(buffer+first_half_index,buffer+first_half_index+1024-sizeof(PageId)-first_half_index,sibling.buffer);
	
	sibling.numKeys = getKeyCount() - count_half_keys;
	sibling.setNextNodePtr(getNextNodePtr());
	
	std::fill(buffer+first_half_index, buffer + 1024- sizeof(PageId), 0); 
	numKeys = count_half_keys;
	
	int first_key;
	std::copy(sibling.buffer,sibling.buffer+sizeof(int),(unsigned char*)&first_key);
	if(key>=first_key) 
	{
		sibling.insert(key, rid);
	}
	else 
	{
		insert(key, rid);
	}
	RecordId siblingRid;
	siblingRid.pid = -1;
	siblingRid.sid = -1;
	std::copy(sibling.buffer,sibling.buffer+sizeof(int),(unsigned char*)&siblingKey);
	memcpy(&siblingRid, sibling.buffer+sizeof(int), sizeof(RecordId));
	return 0;
}
/*
 * Find the entry whose key value is larger than or equal to searchKey
 * and output the eid (entry number) whose key value >= searchKey.
 * Remeber that all keys inside a B+tree node should be kept sorted.
 * @param searchKey[IN] the key to search for
 * @param eid[OUT] the entry number that contains a key larger than or equalty to searchKey
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
	int record_key_size = sizeof(RecordId) + sizeof(int);
	
	char* temp = buffer;
	
	int i=0;
			while(i<getKeyCount()*record_key_size)
	{
		
				
	//	int header = *reinterpret_cast<const int*>(&buffer[4+i]);
		//cout<<"he"<<header<<"--"<<current_key<<"\n";
		//std::copy(&current_key,&current_key +sizeof(int),temp);
		//cout<<"-"<<sizeof(int);
		int current_key;
		std::copy(temp,temp+4,(unsigned char*)&current_key);
		
		if(current_key >= searchKey)
		{
			
			eid = i/record_key_size;
			return 0;
		}
		
		temp += record_key_size; 
		i+=record_key_size;
	}
	
	eid = getKeyCount();
	return 0;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
	int record_key_size = sizeof(RecordId) + sizeof(int);
	
	if(eid >= getKeyCount() || eid < 0)
		return RC_NO_SUCH_RECORD; 
	int current_pos = eid*record_key_size;
	
	char* temp = buffer;
	

		std::copy(temp+current_pos,temp+current_pos+4,(unsigned char*)&key);
	memcpy(&rid, temp+current_pos+sizeof(int), sizeof(RecordId));
	
	return 0;
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{
	PageId pid = 0; 
	char* temp = buffer;
	

			std::copy(temp+1024-sizeof(PageId),temp+1024,(unsigned char*)&pid);

	return pid;
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
	if(pid < 0)
		return RC_INVALID_PID;
	
	char* temp = buffer;
	

				//std::copy(temp+1024-sizeof(PageId),temp+1024,(unsigned char*)&pid);
				
					std::copy(cast(&pid),cast(&pid) + sizeof(PageId),temp+1024-sizeof(PageId));


	return 0;
}


//Nonleaf node constructor
BTNonLeafNode::BTNonLeafNode()
{
	numKeys=0;
	std::fill(buffer, buffer + 1024, 0); //clear the buffer if necessary
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
	RC RC_read;
	RC_read= pf.read(pid, buffer);	
	return RC_read;	
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
	RC RC_write;
	RC_write = pf.write(pid, buffer);
	return RC_write;
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
	
	int page_key_size = sizeof(PageId) + sizeof(int);
	int max_num_keys = (1024-sizeof(PageId))/page_key_size; //127
	int count=0;
	
	char* temp = buffer+8;
	
	
	int i=8;
	while(i<=1016)
	{
		int current_key;
		std::copy(temp,temp+4,(unsigned char*)&current_key);
		if(current_key==0) 
			break;
		count++;

		temp += page_key_size; 
		i+=page_key_size;
	}
	
	return count;
}

/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
	int page_key_size = sizeof(PageId) + sizeof(int);
	int max_num_keys = (1024-sizeof(PageId))/page_key_size; //127
	
	
	if(getKeyCount()+1 > max_num_keys)
	{
		return RC_NODE_FULL;
	}


	char* temp = buffer+8;
	
	
	int i=8;
	while(i+page_key_size<1024)
	{
		int current_key;
		std::copy(temp,temp+4,(unsigned char*)&current_key);
		
		if(current_key==0 || !(key > current_key))
			break;
		
		temp += page_key_size; 
		i+=page_key_size;
	}
	
	char* new_buff = (char*)malloc(1024);
	std::fill(new_buff, new_buff + 1024, 0); 
	
	std::copy(buffer,buffer+i,new_buff);
	std::copy(cast(&key),cast(&key)+sizeof(int),new_buff+i);
	std::copy(cast(&pid),cast(&pid)+sizeof(int),new_buff+i+sizeof(int));
	
	
	std::copy(buffer+i,buffer+i+getKeyCount()*page_key_size - i + 8,new_buff+i+page_key_size);

	std::copy(new_buff,new_buff+1024,buffer);
	
	free(new_buff);
	
	numKeys++;	
	return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{
	int page_key_size = sizeof(PageId) + sizeof(int);
	int max_num_keys = (1024-sizeof(PageId))/page_key_size; 
	
	if(!(getKeyCount() >= max_num_keys))
		return RC_INVALID_FILE_FORMAT;
	
	if(sibling.getKeyCount()!=0)
		return RC_INVALID_ATTRIBUTE;

	std::fill(sibling.buffer, sibling.buffer + 1024, 0); 

	int count_half_keys = ((int)((getKeyCount()+1)/2));


	int first_half_index = count_half_keys*page_key_size + 8;
	
	
	int key1 = -1;
	int key2 = -1;
	
	std::copy(buffer+first_half_index-8,buffer+first_half_index-8+sizeof(int), (unsigned char*)&key1);
		std::copy(buffer+first_half_index,buffer+first_half_index+sizeof(int), (unsigned char*)&key2);

	if(key < key1) 
	{
		
		std::copy(buffer+first_half_index,buffer+first_half_index+1024-first_half_index,sibling.buffer+8);
		sibling.numKeys = getKeyCount() - count_half_keys;
		
		memcpy(&midKey, buffer+first_half_index-8, sizeof(int));
		
		memcpy(sibling.buffer, buffer+first_half_index-4, sizeof(PageId));
		
		
		std::fill(buffer+first_half_index-8, buffer + 1024, 0); 
		numKeys = count_half_keys - 1;
		
		insert(key, pid);		
	}
	else if(key > key2)
	{
		
		std::copy(buffer+first_half_index+8,buffer+first_half_index+8+1024-first_half_index-8,sibling.buffer+8);
		sibling.numKeys = getKeyCount() - count_half_keys - 1;
		
		
		std::copy(buffer+first_half_index,buffer+first_half_index+sizeof(int),&midKey);

		std::copy(buffer+first_half_index+4,buffer+first_half_index+4+sizeof(PageId),sibling.buffer);
		std::fill(buffer+first_half_index, buffer + 1024, 0); 
		numKeys = count_half_keys;
		
		sibling.insert(key, pid);
		
	}
	else 
	{

		std::copy(buffer+first_half_index,buffer+first_half_index+1024-first_half_index,sibling.buffer+8);
		sibling.numKeys = getKeyCount() - count_half_keys;
		
		std::fill(buffer+first_half_index, buffer + 1024, 0); 
		numKeys = count_half_keys;
		
		midKey = key;
		
		
		std::copy(cast(&pid),cast(&pid)+sizeof(PageId),sibling.buffer);
	}

	return 0;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
	int page_key_size = sizeof(PageId) + sizeof(int);
	
	char* temp = buffer+8;	
	
	int i=8;
	while(i-8<getKeyCount()*page_key_size)	
	{
		int current_key;
		std::copy(temp,temp+4,(unsigned char*)&current_key);
				
		if(i==8 && current_key > searchKey) 
		{
	
					std::copy(buffer,buffer+4,(unsigned char*)&pid);

			return 0;
		}
		else if(current_key > searchKey)
		{
			
								std::copy(temp-4,temp-4+4,(unsigned char*)&pid);

			return 0;
		}
		
		temp += page_key_size;
		i+=page_key_size;
	}
	

									std::copy(temp-4,temp-4+4,(unsigned char*)&pid);

	return 0;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
	RC error;

	std::fill(buffer, buffer + 1024, 0); 
	

	char* temp = buffer;
	

	std::copy(cast(&pid1),cast(&pid1)+4,temp);
	
	error = insert(key, pid2);
	
	if(error!=0)
		return error;
	
	
	
	return 0;
}

