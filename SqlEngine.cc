/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"
#include <cstring>
#include <stdlib.h>

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning
  IndexCursor c;	// cursor for navigating through the tree
  BTreeIndex tree; // BTree for indexing
	
  RC     rc;
  int    key;     
  string condition_value;
  int    count = 0;
  int    diff;

  rc = rf.open(table + ".tbl", 'r');
  if (rc < 0) {
	fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
	return rc;
  }
  
	SelCond temp_cond;
	bool has_condition = false; 
	bool has_value_equal_condition = false; 
	bool use_index = false; 
	
	bool condition_ge = false; 
	bool condition_le = false; 
	int max = -1;
	int min = -1;
	int eq_val = -1; 
	
	int condIndex = -1;
	
	bool value_conflict = false;
	std::string val_eq = "";
	
	int i=0;
	
	while(i<cond.size())
	{
		temp_cond = cond[i];
		int temp_val = atoi(temp_cond.value); 

		if(temp_cond.attr==1 && temp_cond.comp!=SelCond::NE)
		{
			has_condition = true; 
			
			switch(temp_cond.comp)
			{
			
			case SelCond::EQ : 
			
				eq_val = temp_val;
				condIndex = i;
				break;
			
			case SelCond::GE : 
			
				if(temp_val > min || min==-1) 
				{
					condition_ge = true;
					min = temp_val;
				}
				break;
			
			case SelCond::GT : 
			
				if(temp_val >= min || min==-1)
				{
					condition_ge = false;
					min = temp_val;
				}
				break;
			
			case SelCond::LE : 
			
				if(temp_val < max || max==-1) 
				{
					condition_le = true;
					max = temp_val;
				}
				break;
			
			case SelCond::LT : 
			
				if(temp_val <= max || max==-1) 
				{
					condition_le = false;
					max = temp_val;
				}
				break;
			}
		}
		else if(temp_cond.attr==2) 
		{
			has_value_equal_condition = true;
			
			if(temp_cond.comp==SelCond::EQ) 
			{
				if(val_eq=="" || strcmp(condition_value.c_str(), cond[i].value)==0) 
					val_eq=temp_val;
				else
					value_conflict = true;
			}
		}
		i++;
	}
	

	if(value_conflict )
		goto end_select_early;
	if((max!=-1 && min!=-1 && max<min))
				goto end_select_early;

	if(max!=-1 && min!=-1 && !condition_ge && !condition_le && max==min)
		goto end_select_early;
  

  if(tree.open(table + ".idx", 'r')!=0 || (!has_condition && attr!=4))
  {
	  // scan the table file from the beginning
	  rid.pid = rid.sid = 0;
	  count = 0;
	  while (rid < rf.endRid()) {
		// read the tuple
		if ((rc = rf.read(rid, key, condition_value)) < 0) {
		  fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
		  goto exit_select;
		}

		// check the conditions on the tuple
		for (unsigned i = 0; i < cond.size(); i++) {
		  // compute the difference between the tuple value and the condition value
		  switch (cond[i].attr) {
		  case 1:
		diff = key - atoi(cond[i].value);
		break;
		  case 2:
		diff = strcmp(condition_value.c_str(), cond[i].value);
		break;
		  }

		  // skip the tuple if any condition is not met
		  switch (cond[i].comp) {
			  case SelCond::EQ:
				if (diff != 0) goto next_tuple;
				break;
			  case SelCond::NE:
				if (diff == 0) goto next_tuple;
				break;
			  case SelCond::GT:
				if (diff <= 0) goto next_tuple;
				break;
			  case SelCond::LT:
				if (diff >= 0) goto next_tuple;
				break;
			  case SelCond::GE:
				if (diff < 0) goto next_tuple;
				break;
			  case SelCond::LE:
				if (diff > 0) goto next_tuple;
				break;
		  }
		}

		// the condition is met for the tuple. 
		// increase matching tuple counter
		count++;

		// print the tuple 
		switch (attr) {
			case 1:  // SELECT key
			  fprintf(stdout, "%d\n", key);
			  break;
			case 2:  // SELECT value
			  fprintf(stdout, "%s\n", condition_value.c_str());
			  break;
			case 3:  // SELECT *
			  fprintf(stdout, "%d '%s'\n", key, condition_value.c_str());
			  break;
		}

		// move to the next tuple
		next_tuple:
		++rid;
	  }
  }
  else 
  {
	count = 0;
	rid.pid = rid.sid = 0;
	use_index = true; 
	
	if(eq_val!=-1) 
		tree.locate(eq_val, c);
	else if(min!=-1 && !condition_ge) 
		tree.locate(min+1, c);
	else if(min!=-1 && condition_ge) 
		tree.locate(min, c);
	else
		tree.locate(0, c);
	
	while(tree.readForward(c, key, rid)==0)
	{
		if(!has_value_equal_condition && attr==4) 
		{
			if(eq_val!=-1 && key!=eq_val) 
				goto end_select_early;
			
			if(max!=-1) 
			{
				if(condition_le && key>max)
					goto end_select_early;
				else if(!condition_le && key>=max)
					goto end_select_early;
			}
			
			if(min!=-1) 
			{
				if(condition_ge && key<min)
					goto end_select_early;
				else if(!condition_ge && key<=min)
					goto end_select_early;
			}
			
			
			count++;
			continue;
		}
	
		if ((rc = rf.read(rid, key, condition_value)) < 0) {
		  fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
		  goto exit_select;
		}

		for (unsigned i = 0; i < cond.size(); i++)
		{
			switch (cond[i].attr)
			{
				case 1:
					diff = key - atoi(cond[i].value);
					break;
				case 2:
					diff = strcmp(condition_value.c_str(), cond[i].value);
					break;
			}


			switch (cond[i].comp)
			{
				case SelCond::EQ:
					if (diff != 0)
					{
						if(cond[i].attr==1)
							goto end_select_early;
						goto continue_while;
					}
					break;
				case SelCond::NE:
					if (diff == 0) goto continue_while; 
					break;
				case SelCond::GT:
					if (diff <= 0) goto continue_while; 
					break;
				case SelCond::LT:
					if (diff >= 0)
					{
						if(cond[i].attr==1) 
							goto end_select_early;
						goto continue_while;
					}
					break;
				case SelCond::GE:
					if (diff < 0) goto continue_while; 
					break;
				case SelCond::LE:
					if (diff > 0)
					{
						if(cond[i].attr==1) 
							goto end_select_early;
						goto continue_while;
					}
					break;
			}
		}

		//tuple curosr++
		count++;

		// print the tuple 
		switch (attr)
		{
			case 1:  
			  fprintf(stdout, "%d\n", key);
			  break;
			case 2:  
			  fprintf(stdout, "%s\n", condition_value.c_str());
			  break;
			case 3:  
			  fprintf(stdout, "%d '%s'\n", key, condition_value.c_str());
			  break;
		}
		
		continue_while:;
	}
  }

  
  end_select_early: 
  
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  exit_select:
  
  if(use_index)
	tree.close();
	
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  RC rc_wierd;

	RecordFile load;

	const char * loadfile_cstring=loadfile.c_str();



	ifstream table_data(loadfile_cstring);

	 if(!table_data.is_open())
			fprintf(stderr, "Error: loadfile %s cannot be opened\n", loadfile.c_str());

	string line;
	int key;
	string val;
	RecordId rid;

	  rc_wierd = load.open(table + ".tbl", 'w');


	  BTreeIndex bt;
	  if(index)
	  {
	    string indexName = table+".idx";

        RC rc = bt.open(indexName,'w');
	  
	 while(getline(table_data, line))
	 	  {
	 		parseLoadLine(line, key, val);
	 		rc_wierd = load.append(key, val, rid);
			if(rc_wierd!=0)
			{
				return RC_FILE_WRITE_FAILED;
			}
			rc = bt.insert(key,rid);
if(rc !=0)
{
	cout<<"shit";
	return rc;
}
			
	 	  }
		  bt.close();

	  }
	  
	  else
	  {
		   while(getline(table_data, line))
	 	  {
	 		parseLoadLine(line, key, val);
	 		rc_wierd = load.append(key, val, rid);
			if(rc_wierd!=0)
			{
				return RC_FILE_WRITE_FAILED;
			}
		  }
	  }
	 load.close();
	   table_data.close();

  return rc_wierd;

}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
