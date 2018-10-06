import numpy as np
from sklearn import preprocessing
from sklearn.base import BaseEstimator, RegressorMixin 
import KernelFunction as kf
import TwinPlane1
import TwinPlane2

#__copyright__ = ""
#__license__ = "GPL"
# __version__ = "1.1"
# __maintainer__ = "Arnav Kansal"
# __email__ = "ee1130440@ee.iitd.ac.in"
# __status__ = "Production"

class TwinSVMRegressor(BaseEstimator, RegressorMixin):
    def __init__(self,Epsilon1=0.1, Epsilon2=0.1, C1=1, C2=1,kernel_type=0,kernel_param=1,regulz1=0.0001, regulz2=0.0001,_estimator_type="regressor"):
        self.Epsilon1=Epsilon1
        self.Epsilon2=Epsilon2
        self.C1=C1
        self.C2=C2
        self.regulz1 = regulz1
        self.regulz2 = regulz2
        self.kernel_type=kernel_type
        self.kernel_param=kernel_param
        
    def fit(self, X, Y):
        Y=Y.values.reshape(len(Y),1)
        assert (type(self.Epsilon1) in [float,int,long])
        assert (type(self.Epsilon2) in [float,int,long])
        assert (type(self.C1) in [float,int,long])
        assert (type(self.C2) in [float,int,long])
        assert (type(self.regulz1) in [float,int,long])
        assert (type(self.regulz2) in [float,int,long])
        assert (type(self.kernel_param) in [float,int,long])
        assert (self.kernel_type in [0,1,2,3])
        r_x,c=X.shape
        r_y=Y.shape[0]
        assert (r_x==r_y)
        r=r_x
        




        e=np.ones((r,1))
        
        if(self.kernel_type==0): # no need to cal kernel
            H = np.hstack((X,e))
        else:
            H = np.zeros((r,r))
            
            for i in range(r):
                for j in range(r):
                    H[i][j] = kf.kernelfunction(self.kernel_type,X[i],X[j],self.kernel_param)
            H = np.hstack((H,e))
            
        #####################Calculation of Function Parameters(Equation of planes) 
        print H
        [w1,b1] = TwinPlane1.Twin_plane_1(H,Y,self.C1,self.Epsilon1,self.regulz1)
        [w2,b2] = TwinPlane2.Twin_plane_2(H,Y,self.C2,self.Epsilon2,self.regulz2)
        self.plane1_coeff_ = w1
        self.plane1_offset_ = b1
        self.plane2_coeff_ = w2
        self.plane2_offset_ = b2
        self.data_ = X

        np.savetxt("w1.txt",w1)
        np.savetxt("b1.txt",b1)
        np.savetxt("w2.txt",w2)
        np.savetxt("b2.txt",b2)


        return self


    def get_params(self, deep=True):
        return {"Epsilon1": self.Epsilon1, "Epsilon2": self.Epsilon2, "C1": self.C1, "C2": self.C2, "regulz1": self.regulz1,
                "regulz2":self.regulz2, "kernel_type": self.kernel_type, "kernel_param": self.kernel_param}

    def set_params(self, **parameters):
        for parameter, value in parameters.items():
            #self.setattr(parameter, value)
            setattr(self,parameter, value)
        return self


    def predict(self, X):
        #X_test = preprocessing.scale(X)    
        if(self.kernel_type==0): # no need to cal kernel
            S = X
        else:
            S = np.zeros((X.shape[0],self.data_.shape[0]))
            for i in range(X.shape[0]):
                for j in range(self.data_.shape[0]):
                    S[i][j] = kf.kernelfunction(self.kernel_type,X[i],self.data_[j].T,self.kernel_param)


        
        y1 = np.dot(S,self.plane1_coeff_)+ ((self.plane1_offset_)*(np.ones((X.shape[0],1))))

        y2 = np.dot(S,self.plane2_coeff_)+ ((self.plane2_offset_)*(np.ones((X.shape[0],1))))

        ###############Compute test data predictions

        return (y1+y2)/2    


