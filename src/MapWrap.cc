#include "MapWrap.hh"
#include <stdio.h>
#include<string.h>
using namespace std;

template <typename A, typename B>
multimap<B, A> flip_map(map<A,B> & src) {

    multimap<B,A> dst;
    typename map<A, B>::const_iterator itrs;
    for(itrs= src.begin(); itrs != src.end(); ++itrs)
        dst.insert(pair<B, A>(itrs -> second, itrs -> first));

    return dst;
}

MapWrap::MapWrap(int i){
	map <string, map <int, int> > mymap;
	this->cMap = mymap;
}

void MapWrap::mp_insert(const char *pred, const double tm_interval){

	string c_pred(pred);
	int c_succ;
	c_succ = tm_interval;
	
	map<int, int> inner_map = cMap[c_pred];
	inner_map[c_succ]++;
	cMap[c_pred] = inner_map;

}

const int MapWrap::get_value(const char *pred){
	string c_pred(pred);
	map<string, map<int, int> >::iterator it;
	it=cMap.find(c_pred);
	int pred_interval = 0;
	if(it!=cMap.end()){
		map<int, int> inner_map;
		inner_map = it->second;
		multimap<int, int> reverse_map = flip_map(inner_map);
		multimap<int, int>::const_reverse_iterator rit = reverse_map.rbegin();
		pred_interval = rit->second;
	}
	return pred_interval;
}
