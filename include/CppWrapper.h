typedef void WrapperMap;
typedef void OnlyMap; 
typedef void PredictMap;

#ifdef __cplusplus
extern "C" {
#endif
	WrapperMap * map_new(int i);
	void map_insert(const WrapperMap *test, const char *pred, const double tm_interval);
	const int map_get_value(const WrapperMap *test, const char *pred);
	void map_delete(WrapperMap *test);

#ifdef __cplusplus
}
#endif