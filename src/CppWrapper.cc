#include "CppWrapper.h"
#include "MapWrap.hh"

extern "C" {

	WrapperMap * map_new(int i) {
		MapWrap *t = new MapWrap(i);

		return (WrapperMap *)t;
	}

	void map_insert(const WrapperMap *test, const char *pred, const double tm_interval) {
		MapWrap *t = (MapWrap*)test;
		t->mp_insert(pred, tm_interval);
	}

	const int map_get_value(const WrapperMap *test, const char *pred){
		MapWrap *t = (MapWrap*)test;
		return t->get_value(pred);
	}

	void map_delete(WrapperMap *test) {
		MapWrap *t = (MapWrap *)test;

		delete t;
	}

}