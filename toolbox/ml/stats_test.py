# -*- coding: utf-8 -*-
from toolbox import spark_utils
import unittest
import pandas as pd
from toolbox.ml import stats
import numpy as np
from scipy import stats as st
import csv
import subprocess

class TestStatsMethods(unittest.TestCase):

    # Initialisation for each test case
    def setUp(self):
        sc, sqlContext = spark_utils.sparkConnect(3,4,"test ml stats",False)
        self.sc = sc
        self.sqlContext = sqlContext

    # Terminaison for each test case
    def tearDown(self):
        self.sc.stop()

    def test_twoSampleKSTest_same_dataframe(self):
        array1 = np.array([-1.1,-1.2,-2.4,-0.5,5.4,3.5,4.2,4.3,4.2,3.3,3.6,4,3,4,-1,-1.4,5.6,7.6,-1.2,4])
        array2 = np.array([11,13,4.5,4.8,8,6.7,7.9,6,5,7,6.7,6.7,8,9,8.7,8.7,8.4,6,11,11.5])
        pd_test = pd.DataFrame({"var1" : pd.Series(array1),
                       "var2" : pd.Series(array2)})
        df_test = self.sqlContext.createDataFrame(pd_test)
        n = pd_test.shape[0]
        v1 = "var1"
        v2 = "var2"
        alpha = 0.05
        c_alpha = np.sqrt((-1./2.)*np.log(alpha/2.))
        threshold = c_alpha*np.sqrt((n+n)*1./(n*n))
        res = stats.twoSampleKSTest(df_test, [v1,v2], alpha=alpha)
        res_scipy = st.ks_2samp(array1, array2) # [D, p_value]
        self.assertEqual(res[0][0],v1)
        self.assertEqual(res[0][1],v2)
        self.assertEqual(res[0][2],alpha)
        self.assertEqual(res[0][3],threshold)
        self.assertEqual(round(res[0][4],2),round(res_scipy[0],2))
        # If D > threshold null hypothisis rejected
        # If alpha > p_value from scipy null hypothesis rejected
        self.assertEqual(res[0][4] >= threshold, alpha >= res_scipy[1])

    def test_twoSampleKSTest_different_dataframes_several_features(self):
        array1 = 2.5 * np.random.randn(1000) + 3
        array2 = np.random.randn(1000)
        # array3 has the same distribution than array 1 but different than array2
        array3 = 2.5 * np.random.randn(900) + 3
        pd_test1 = pd.DataFrame({"var11" : pd.Series(array1),
                                 "var12" : pd.Series(array2)})
        df_test1 = self.sqlContext.createDataFrame(pd_test1)
        pd_test2 = pd.DataFrame({"var2" : pd.Series(array3)})
        df_test2 = self.sqlContext.createDataFrame(pd_test2)
        n1 = pd_test1.shape[0]
        n2 = pd_test2.shape[0]
        v11 = "var11"
        v12 = "var12"
        v2 = "var2"
        alpha = 0.05
        c_alpha = np.sqrt((-1./2.)*np.log(alpha/2.))
        threshold = c_alpha*np.sqrt((n1+n2)*1./(n1*n2))
        res = stats.twoSampleKSTest(df_test1, [v11, v12], df_test2, [v2], alpha=alpha)
        res_scipy_1_3 = st.ks_2samp(array1, array3) # [D, p_value]
        res_scipy_2_3 = st.ks_2samp(array2, array3) # [D, p_value]
        # If D > threshold null hypothisis rejected
        # If alpha > p_value from scipy null hypothesis rejected
        # Test between array1 and array2
        self.assertEqual(res[0][0],v11)
        self.assertEqual(res[0][1],v2)
        self.assertEqual(res[0][2],alpha)
        self.assertEqual(res[0][3],threshold)
        self.assertEqual(round(res[0][4],2),round(res_scipy_1_3[0],2))
        self.assertEqual(res[0][4] >= threshold, alpha >= res_scipy_1_3[1])
        self.assertTrue(res[0][5])
        # Test between array1 and array3
        self.assertEqual(res[1][0],v12)
        self.assertEqual(res[1][1],v2)
        self.assertEqual(res[1][2],alpha)
        self.assertEqual(res[1][3],threshold)
        self.assertEqual(round(res[1][4],2),round(res_scipy_2_3[0],2))
        self.assertEqual(res[1][4] >= threshold, alpha >= res_scipy_2_3[1])
        self.assertFalse(res[1][5])

    def test_reportTwoSampleKSTestCsv(self):
        array1 = 2.5 * np.random.randn(1000) + 3
        array2 = np.random.randn(1000)
        # array3 has the same distribution than array 1 but different than array2
        array3 = 2.5 * np.random.randn(900) + 3
        pd_test1 = pd.DataFrame({"var11" : pd.Series(array1),
                                 "var12" : pd.Series(array2)})
        df_test1 = self.sqlContext.createDataFrame(pd_test1)
        pd_test2 = pd.DataFrame({"var2" : pd.Series(array3)})
        df_test2 = self.sqlContext.createDataFrame(pd_test2)
        v11 = "var11"
        v12 = "var12"
        v2 = "var2"
        alpha = 0.05
        res = stats.twoSampleKSTest(df_test1, [v11, v12], df_test2, [v2], alpha=alpha)
        stats.reportTwoSampleKSTestCsv(res, "test_reportTwoSampleKSTestCsv.csv")
        f = open("test_reportTwoSampleKSTestCsv.csv",'rb')
        reader = csv.reader(f)
        nb_lines = 0
        for i, row in enumerate(reader):
            if(i==0):
                self.assertEqual(row[0], "Variable1;Variable2;pValue;Threshold;D;Result")
            else:
                self.assertEqual(len(row[0].split(';')),6)
            nb_lines+=1
        self.assertEqual(nb_lines,3)
        subprocess.call(['rm','test_reportTwoSampleKSTestCsv.csv'])

if __name__ == '__main__':
    unittest.main()
