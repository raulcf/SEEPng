package algostest;

import api.SeepProgram;
import api.lviews.LogicalView;
import api.objects.DenseMatrix;
import api.topology.Cluster;

public class SUMMA implements SeepProgram {

	@Override
	public void configure(Cluster c) {
		// Add a cluster impl
		api.addCluster(c);
	}
	
	@Override
	public void program() {
		
		// Read matrices from storage into logical views
		LogicalView<DenseMatrix> matrixA = api.readFromPath("/data/matrixA");
		LogicalView<DenseMatrix> matrixB = api.readFromPath("/data/matrixB");
		
		// Create a logicalView for the result of the operation
		LogicalView<DenseMatrix> matrixC = api.createLogicalView();
		
		// Distribute matrices in the cluster according to some layout
//		api.blockCyclicDistribution(matrixA);
//		api.blockCyclicDistribution(matrixB);
		
		// Recover data-specific information (metadata stored in the storage manager)
		int n = api.gridCols();
		
		// Implementation of parallel SUMMA algorithm
		for(int k = 0; k < n; k++) {
			for(int i = 0; i < api.gridRows(); i++) {
				for(int j = 0; j < api.gridCols(); j++) {
					DenseMatrix multiplied = matrixA.position(i, k).multiply(matrixB.position(k, j));
					DenseMatrix added = matrixC.position(i, j).addMatrix(multiplied);
					matrixC.assign(added, i, j);
				}
			}
		}
		
		// Finally write result back to disk (or something)
		api.writeToPath(matrixC, "/data/matrixC");
	}
	
	// Optionally specify a custom partitioner
//	Partitioner p = null;
//	api.partitionAndBlockCyclicDistribution(matrixA, API.c, p);

}
