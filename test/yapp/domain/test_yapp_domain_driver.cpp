#include <cppunit/CompilerOutputter.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/TestRunner.h>
#include <cppunit/BriefTestProgressListener.h>

int main (int argc, char* argv[]) {
  // informs test-listener about testresults
  CPPUNIT_NS::TestResult test_result;

  // register listener for collecting the test-results
  CPPUNIT_NS::TestResultCollector collected_results;
  test_result.addListener (&collected_results);

  // register listener for per-test progress output
  CPPUNIT_NS::BriefTestProgressListener progress;
  test_result.addListener (&progress);

  // insert test-suite at test-runner by registry
  CPPUNIT_NS::TestRunner test_runner;
  test_runner.addTest (CPPUNIT_NS::TestFactoryRegistry::getRegistry ().makeTest ());
  test_runner.run (test_result);

  // output results in compiler-format
  CPPUNIT_NS::CompilerOutputter compiler_outputter (&collected_results, std::cerr);
  compiler_outputter.write ();

  // return 0 if tests were successful
  return collected_results.wasSuccessful () ? 0 : 1;
}
