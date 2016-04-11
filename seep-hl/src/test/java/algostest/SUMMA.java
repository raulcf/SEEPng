package algostest;

import java.util.Set;

import api.API;
import api.SeepProgram;
import api.lviews.LogicalView;
import api.objects.DenseMatrix;

public class SUMMA implements SeepProgram {

	@Override
	public void program() {
		
		// Read matrices from storage into logical views
		LogicalView<DenseMatrix> matrixA = api.readFromPath("/data/matrixA");
		LogicalView<DenseMatrix> matrixB = api.readFromPath("/data/matrixB");
		
		// Create a logicalView for the result of the operation
		LogicalView<DenseMatrix> matrixC = api.createLogicalView();
		
		// Distribute matrices in the cluster according to some layout
		api.blockCyclicDistribution(matrixA, API.c);
		api.blockCyclicDistribution(matrixB, API.c);
		
		// Recover data-specific information (metadata stored in the storage manager)
		int n = matrixA.getMetadata(DenseMatrix.N);
		
		// Implementation of parallel SUMMA algorithm
		for(int k = 0; k < n; k++) {
			for(int i = 0; i < API.c.numRows; i++) {
				for(int j = 0; j < API.c.numCols; j++) {
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
