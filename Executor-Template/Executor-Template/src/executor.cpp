/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "executor.h"

#include <exceptions/buffer_exceeded_exception.h>
#include <cmath>
#include <ctime>
#include <functional>
#include <iostream>
#include <string>
#include <utility>
#include <sstream>
#include <algorithm>
#include <map>
#include <vector>

#include "file_iterator.h"
#include "page_iterator.h"
#include "storage.h"

using namespace std;

namespace badgerdb {

void TableScanner::print() const {
  badgerdb::File file = badgerdb::File::open(tableFile.filename());
  for (badgerdb::FileIterator iter = file.begin(); iter != file.end(); ++iter) {
    badgerdb::Page page = *iter;
    badgerdb::Page* buffered_page;
    bufMgr->readPage(&file, page.page_number(), buffered_page);

    for (badgerdb::PageIterator page_iter = buffered_page->begin();
         page_iter != buffered_page->end(); ++page_iter) {
      string key = *page_iter;
      string print_key = "(";
      int current_index = 0;
      for (int i = 0; i < tableSchema.getAttrCount(); ++i) {
        switch (tableSchema.getAttrType(i)) {
          case INT: {
            int true_value = 0;
            for (int j = 0; j < 4; ++j) {
              if (std::string(key, current_index + j, 1)[0] == '\0') {
                continue;  // \0 is actually representing 0
              }
              true_value +=
                  (std::string(key, current_index + j, 1))[0] * pow(128, 3 - j);
            }
            print_key += to_string(true_value);
            current_index += 4;
            break;
          }
          case CHAR: {
            int max_len = tableSchema.getAttrMaxSize(i);
            print_key += std::string(key, current_index, max_len);
            current_index += max_len;
            current_index +=
                (4 - (max_len % 4)) % 4;  // align to the multiple of 4
            break;
          }
          case VARCHAR: {
            int actual_len = key[current_index];
            current_index++;
            print_key += std::string(key, current_index, actual_len);
            current_index += actual_len;
            current_index +=
                (4 - ((actual_len + 1) % 4)) % 4;  // align to the multiple of 4
            break;
          }
        }
        print_key += ",";
      }
      print_key[print_key.size() - 1] = ')';  // change the last ',' to ')'
      cout << print_key << endl;
    }
    bufMgr->unPinPage(&file, page.page_number(), false);
  }
  bufMgr->flushFile(&file);
}

JoinOperator::JoinOperator(const File& leftTableFile,
                           const File& rightTableFile,
                           const TableSchema& leftTableSchema,
                           const TableSchema& rightTableSchema,
                           const Catalog* catalog,
                           BufMgr* bufMgr)
    : leftTableFile(leftTableFile),
      rightTableFile(rightTableFile),
      leftTableSchema(leftTableSchema),
      rightTableSchema(rightTableSchema),
      resultTableSchema(
          createResultTableSchema(leftTableSchema, rightTableSchema)),
      catalog(catalog),
      bufMgr(bufMgr),
      isComplete(false) {
  // nothing
}

TableSchema JoinOperator::createResultTableSchema(
    const TableSchema& leftTableSchema,
    const TableSchema& rightTableSchema) {
  vector<Attribute> attrs;

  // first add all the left table attrs to the result table
  for (int k = 0; k < leftTableSchema.getAttrCount(); ++k) {
    Attribute new_attr = Attribute(
        leftTableSchema.getAttrName(k), leftTableSchema.getAttrType(k),
        leftTableSchema.getAttrMaxSize(k), leftTableSchema.isAttrNotNull(k),
        leftTableSchema.isAttrUnique(k));
    attrs.push_back(new_attr);
  }

  // test every right table attrs, if it doesn't have the same attr(name and
  // type) in the left table, then add it to the result table
  for (int i = 0; i < rightTableSchema.getAttrCount(); ++i) {
    bool has_same = false;
    for (int j = 0; j < leftTableSchema.getAttrCount(); ++j) {
      if ((leftTableSchema.getAttrType(j) == rightTableSchema.getAttrType(i)) &&
          (leftTableSchema.getAttrName(j) == rightTableSchema.getAttrName(i))) {
        has_same = true;
      }
    }
    if (!has_same) {
      Attribute new_attr = Attribute(
          rightTableSchema.getAttrName(i), rightTableSchema.getAttrType(i),
          rightTableSchema.getAttrMaxSize(i), rightTableSchema.isAttrNotNull(i),
          rightTableSchema.isAttrUnique(i));
      attrs.push_back(new_attr);
    }
  }
  return TableSchema("TEMP_TABLE", attrs, true);
}

void JoinOperator::printRunningStats() const {
  cout << "# Result Tuples: " << numResultTuples << endl;
  cout << "# Used Buffer Pages: " << numUsedBufPages << endl;
  cout << "# I/Os: " << numIOs << endl;
}

 
vector<Attribute> JoinOperator::getCommonAttributes(
    const TableSchema& leftTableSchema,
    const TableSchema& rightTableSchema) const {
  vector<Attribute> common_attrs;
  //�ж������������ 
  for (int i = 0; i < rightTableSchema.getAttrCount(); ++i) {
    for (int j = 0; j < leftTableSchema.getAttrCount(); ++j) {
      if ((leftTableSchema.getAttrType(j) == rightTableSchema.getAttrType(i)) &&
          (leftTableSchema.getAttrName(j) == rightTableSchema.getAttrName(i))) {
        Attribute new_attr = Attribute(rightTableSchema.getAttrName(i),
                                       rightTableSchema.getAttrType(i),
                                       rightTableSchema.getAttrMaxSize(i),
                                       rightTableSchema.isAttrNotNull(i),
                                       rightTableSchema.isAttrUnique(i));
        common_attrs.push_back(new_attr);
      }
    }
  }
  return common_attrs;
}

string JoinOperator::joinTuples(string leftTuple,
                                string rightTuple,
                                const TableSchema& leftTableSchema,
                                const TableSchema& rightTableSchema) const {
  int cur_right_index = 0;  // current substring index in the right table key
  string result_tuple = leftTuple;

  for (int i = 0; i < rightTableSchema.getAttrCount(); ++i) {
    bool has_same = false;
    for (int j = 0; j < leftTableSchema.getAttrCount(); ++j) {
      if ((leftTableSchema.getAttrType(j) == rightTableSchema.getAttrType(i)) &&
          (leftTableSchema.getAttrName(j) == rightTableSchema.getAttrName(i))) {
        has_same = true;
      }
    }
    // if the key is only owned by right table, add it to the result tuple
    switch (rightTableSchema.getAttrType(i)) {
      case INT: {
        if (!has_same) {
          result_tuple += std::string(rightTuple, cur_right_index, 4);
        }
        cur_right_index += 4;
        break;
      }
      case CHAR: {
        int max_len = rightTableSchema.getAttrMaxSize(i);
        if (!has_same) {
          result_tuple += std::string(rightTuple, cur_right_index, max_len);
        }
        cur_right_index += max_len;
        unsigned align_ = (4 - (max_len % 4)) % 4;  // align to the multiple of
                                                    // 4
        for (int k = 0; k < align_; ++k) {
          result_tuple += "0";
          cur_right_index++;
        }
        break;
      }
      case VARCHAR: {
        int actual_len = rightTuple[cur_right_index];
        result_tuple += std::string(rightTuple, cur_right_index, 1);
        cur_right_index++;
        if (!has_same) {
          result_tuple += std::string(rightTuple, cur_right_index, actual_len);
        }
        cur_right_index += actual_len;
        unsigned align_ =
            (4 - ((actual_len + 1) % 4)) % 4;  // align to the multiple of 4
        for (int k = 0; k < align_; ++k) {
          result_tuple += "0";
          cur_right_index++;
        }
        break;
      }
    }
  }
  return result_tuple;
}

bool OnePassJoinOperator::execute(int numAvailableBufPages, File& resultFile) {
  if (isComplete)
    return true;

  numResultTuples = 0;
  numUsedBufPages = 0;
  numIOs = 0;

  // TODO: Implement the one-pass join algorithm (NOT required in project 3)

  isComplete = true;
  return true;
}

void handle_tuple(string& hashString, vector<string> sameName, string &tup, string& last ,const TableSchema& tableSchema){
    last.insert(last.size(), tup);  // 
    unsigned int already_delete = 0;
    for(unsigned int i = 0; i < sameName.size(); i++){
        string comAttr = sameName[i];  // ith common attribution
        unsigned int rank = tableSchema.getAttrNum(comAttr);
        int index = 0;
        for(unsigned int j = 0; j <= rank; j++){
            DataType dataType = tableSchema.getAttrType(j);
            if(dataType == 0){  // INT 
                if(j == rank){
                    hashString.insert(hashString.size(), 1, tup[index]);  // read the 4 byte from the tuple
                    hashString.insert(hashString.size(), 1, tup[index + 1]);
                    hashString.insert(hashString.size(), 1, tup[index + 2]);
                    hashString.insert(hashString.size(), 1, tup[index + 3]);
                    last.erase(index - already_delete, 4);
                    already_delete += 4;
                }
                index += 4;
            }
            else if(dataType == 1){  // CAHR(n)
                if(j == rank){
                    int k = 0;
                    for(k = 0; k < tableSchema.getAttrMaxSize(j); k++){
                        hashString += tup[index + k];
                    }
                    last.erase(index - already_delete, k);
                    already_delete += k;
                }
                index += tableSchema.getAttrMaxSize(j);
            }
            else if(dataType == 2){  // VARCAHR(n)
                stringstream ss;
                ss << tup[index];
                int size  = 0;  // save the length of the value
                ss >> size;
                if (j == rank){
                    int k = 0;
                    for(k = 0; k < size; k++){
                        hashString += tup[index + 1 + k];
                    }
                    last.erase(index - already_delete, k + 1);
                    already_delete += k + 1;
                }
                index += size + 1;
            }
        }
        if(index % 4 != 0){  // make sure the space is mutiple of 4
            last.erase(index - already_delete, (4 - (index % 4)));
            already_delete += 4 - (index % 4);
            index += 4 - (index % 4);

        }

    }
}

bool NestedLoopJoinOperator::execute(int numAvailableBufPages, File& resultFile) {
    if (isComplete)
        return true;

    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0;
    //io�Ѿ��������page 
    vector<PageId> usedPage;
    //��buf�����ڴ����page 
    vector<Page> already_in_buf;
    vector<string> attrname;
    vector<string> sameName;
    map<string, vector<string>> hashMap;// recordid -> {hashstirng, last/*contains head*/}
    
	//����ͬ��������������ʱ��Ҫ���� 
    for(int i = 0; i < leftTableSchema.getAttrCount(); i++){
        attrname.push_back(leftTableSchema.getAttrName(i));
    }
    for(int i = 0; i < rightTableSchema.getAttrCount(); i++){
        if(count(attrname.begin(), attrname.end(), rightTableSchema.getAttrName(i))){
           sameName.push_back(rightTableSchema.getAttrName(i));
        }
    }
    //first read min(M-1, page.size)'s rightTable
    //���ϵ���ҹ�ϵB(R) > B(S) 
    //ÿ�ζ���һ��R��ÿ�ζ���M-1��S 
	badgerdb::File rightfile = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId(rightTableSchema.getTableName())));
    badgerdb::File leftfile = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId(leftTableSchema.getTableName())));
    int read_page_num = 0;
    int usedPageNum = 0;
    int sum =  0;
    
    //��¼�ļ����� 
    for (FileIterator iter = rightfile.begin(); iter != rightfile.end(); ++iter)
	{
        sum++;
    }
    while(usedPageNum < sum)
	{
    for (FileIterator iter = rightfile.begin(); iter != rightfile.end(); ++iter)
	{
    	//����M-1ҳ����break 
        if(read_page_num >= numAvailableBufPages - 1)
            break;
        PageId pagenum = (*iter).page_number();
        //���Ѿ��������continue 
        if(count(usedPage.begin(), usedPage.end(), pagenum))
            continue;
        //����page 
        Page *new_page;
        bufMgr->readPage(&rightfile, pagenum, new_page);
        //����ǰҳ��ÿ��Ԫ�� 
        for (PageIterator page_iter = (*new_page).begin();page_iter != (*new_page).end();++page_iter)
		{
            string righttuple = *page_iter;
            string last, hashString;
            handle_tuple(hashString, sameName, righttuple, last, rightTableSchema); //get the key(hashString) and the value(last)
			      if(hashMap.count(hashString) == 1){
                hashMap[hashString].push_back(last);
            }
            else{
                vector<string> stringList;
                stringList.push_back(last);
                hashMap.insert(pair<string, vector<string>> (hashString, stringList));
            }

        }
        
        usedPage.push_back(pagenum);
        already_in_buf.push_back(*new_page);
        numIOs++;
        numUsedBufPages++;
        read_page_num++;
    }
    usedPageNum += read_page_num;

    for (FileIterator iter = leftfile.begin(); iter != leftfile.end(); ++iter)
	{
        Page *left_new_page;
        Page p = *iter;
        //���ϵÿ�ζ�һ��page 
		PageId leftPagenum = p.page_number();
        bufMgr->readPage(&leftfile, leftPagenum, left_new_page);
        p = *left_new_page;
        numUsedBufPages++;
        numIOs++;
        //����ǰҳ��ÿ��Ԫ�� 
        for (PageIterator page_iter = p.begin();page_iter != p.end();++page_iter){
            string lefttuple = *page_iter;
            string last, hashString;
            handle_tuple(hashString, sameName, lefttuple, last, leftTableSchema);
            if(hashMap.count(hashString) == 1){
                vector<string> same = hashMap[hashString];
                for(unsigned int i = 0; i < same.size(); i++){
                    numResultTuples++;
                    string temp = same[i];
                    string resultString = lefttuple.insert(lefttuple.size(), temp);
                    HeapFileManager::insertTuple(resultString, resultFile, bufMgr);
                }
            }
        }
        bufMgr->unPinPage(&leftfile, leftPagenum, false);

    }
    for(unsigned int i = 0; i < already_in_buf.size(); i++){
        bufMgr->unPinPage(&rightfile, already_in_buf[i].page_number(), false);
    }
    bufMgr->flushFile(&rightfile);
    already_in_buf.clear();
    read_page_num = 0;
    }

    isComplete = true;
    return true;
}

BucketId GraceHashJoinOperator::hash(const string& key) const {
  std::hash<string> strHash;
  return strHash(key) % numBuckets;
}

bool GraceHashJoinOperator::execute(int numAvailableBufPages,
                                    File& resultFile) {
  if (isComplete)
    return true;

  numResultTuples = 0;
  numUsedBufPages = 0;
  numIOs = 0;

  // TODO: Implement the Grace hash join algorithm (NOT required in project 3)

  isComplete = true;
  return true;
}

}  // namespace badgerdb
