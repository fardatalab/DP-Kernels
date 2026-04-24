#pragma once

#include <math.h>
#include <stdlib.h>

typedef struct Statistics {
	double Mean;
	double Variance;
	double StandardDeviation;
	double StandardError;
	double Min;
	double Max;
} Statistics;

typedef struct Percentiles {
	double P50;
	double P90;
	double P99;
	double P99p9;
	double P99p99;
	double P99p999;
	double P99p9999;
} Percentiles;

double square_root(double x) {
	double guess = x / 2.0;
	double prev_guess = 0.0;
	const double tolerance = 0.00001;

	while (fabs(guess - prev_guess) > tolerance) {
		prev_guess = guess;
		guess = (guess + x / guess) / 2.0;
	}

	return guess;
}

void quick_sort(double arr[], int left, int right) {
	if (left < right) {
		int pivot_index = left + (right - left) / 2;
		double pivot_value = arr[pivot_index];
		int i = left;
		int j = right;
		while (i <= j) {
			while (arr[i] < pivot_value) {
				i++;
			}
			while (arr[j] > pivot_value) {
				j--;
			}
			if (i <= j) {
				// swap arr[i] and arr[j]
				double temp = arr[i];
				arr[i] = arr[j];
				arr[j] = temp;
				i++;
				j--;
			}
		}
		quick_sort(arr, left, j);
		quick_sort(arr, i, right);
	}
}

double
ComputeVariance(
		double* Measurements,
		size_t Length,
		double Mean
	       ) {
	if (Length <= 1) {
		return 0;
	}
	double Variance = 0;
	for (size_t i = 0; i < Length; i++) {
		uint32_t Value = Measurements[i];
		Variance += (Value - Mean) * (Value - Mean) / (Length - 1);
	}
	return Variance;
}

int cmpfunc (const void * a, const void * b) {
	return (*(double*)a - *(double*)b);
}

static void
GetStatistics(
		double* Data,
		size_t DataLength,
		Statistics* AllStatistics,
		Percentiles* PercentileStats
	     ) {
	if (DataLength == 0) {
		return;
	}

	double Sum = 0;
	double Min = 0xFFFFFFFF;
	double Max = 0;
	for (size_t i = 0; i < DataLength; i++) {
		double Value = Data[i];
		Sum += Value;
		if (Value > Max) {
			Max = Value;
		}
		if (Value < Min) {
			Min = Value;
		}
	}
	double Mean = Sum / (double)DataLength;
	double Variance =
		ComputeVariance(
				Data,
				DataLength,
				Mean);
	double StandardDeviation = square_root(Variance);
	double StandardError = StandardDeviation / square_root((double)DataLength);
	AllStatistics->Mean = Mean;
	AllStatistics->Variance = Variance;
	AllStatistics->StandardDeviation = StandardDeviation;
	AllStatistics->StandardError = StandardError;
	AllStatistics->Min = Min;
	AllStatistics->Max = Max;

	/*
	qsort(
			Data,
			DataLength,
			sizeof(double),
			cmpfunc
	     );
	*/
	quick_sort(Data, 0, DataLength -1);

	uint32_t PercentileIndex = (uint32_t)(DataLength * 0.5);
	PercentileStats->P50 = Data[PercentileIndex];

	PercentileIndex = (uint32_t)(DataLength * 0.9);
	PercentileStats->P90 = Data[PercentileIndex];

	PercentileIndex = (uint32_t)(DataLength * 0.99);
	PercentileStats->P99 = Data[PercentileIndex];

	PercentileIndex = (uint32_t)(DataLength * 0.999);
	PercentileStats->P99p9 = Data[PercentileIndex];

	PercentileIndex = (uint32_t)(DataLength * 0.9999);
	PercentileStats->P99p99 = Data[PercentileIndex];

	PercentileIndex = (uint32_t)(DataLength * 0.99999);
	PercentileStats->P99p999 = Data[PercentileIndex];

	PercentileIndex = (uint32_t)(DataLength * 0.999999);
	PercentileStats->P99p9999 = Data[PercentileIndex];
}
