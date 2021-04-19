package com.zhanziwei.hive.udtf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;


class Binary{
    private static void InsertSort(int[] arr) {
        int len = arr.length;
        for(int i = 1; i < len; i++) {
            int temp = arr[i];
            int j;
            for(j = i-1; j >= 0; j--) {
                if(arr[j] > temp) arr[j+1] = arr[j];
                else break;
            }
            arr[j+1] = temp;
        }
    }
    static void quickSort(int[] arr, int low, int high) {
        if(low >= high) return;
        int i = low, j = high, index = arr[low];
        while(i < j) {
            while(i < j && arr[j]>= index) j--;
            if(i < j) arr[i++] = arr[j];
            while(i < j && arr[i] < index) i++;
            if(i < j) arr[j--] = arr[i];
        }
        arr[i] = index;
        quickSort(arr, low, i-1);
        quickSort(arr, i+1, high);
    }
    private static void BubbleSort(int[] arr) {
        int len = arr.length;
        for(int i = 0; i < len; i++) {
            for(int j = 0; j < len - 1 - i; j++) {
                if(arr[j]>arr[j+1]) {
                    int temp = arr[j+1];
                    arr[j+1] = arr[j];
                    arr[j] = temp;
                }
            }
        }
    }
    private static void SelectSort(int[] arr) {
        int len = arr.length;
        int min;
        for(int i = 0; i < len; i++) {
            min = i;
            for(int j = i+1; j < len; j++) {
                if(arr[min]>arr[j]) min = j;
            }
            int temp = arr[min];
            arr[min] = arr[i];
            arr[i] = temp;
        }
    }
    public static int binarySearchLeft(int[] nums, int target) {
        int left = 0;
        int right = nums.length - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (nums[mid] < target) {
                // 搜索区间变为 [mid+1, right]
                left = mid + 1;
            }
            if (nums[mid] > target) {
                // 搜索区间变为 [left, mid-1]
                right = mid - 1;
            }
            if (nums[mid] == target) {
                // 收缩右边界
                right = mid - 1;
            }
        }
        // 检查是否越界
        if (left >= nums.length || nums[left] != target)
            return -1;
        return left;
    }
    public static void main(String[] args) {
        int[] arr = new int[]{5,4,3,2,1};
        BubbleSort(arr);
        System.out.println(Arrays.toString(arr));
    }
}
public class Main {
    static int M=10000;//(此路不通)
    public static void main(String[] args) {
        // TODO Auto-generated method stub
//        int[][] weight1 = {//邻接矩阵
//                {0,3,2000,7,M},
//                {3,0,4,2,M},
//                {M,4,0,5,4},
//                {7,2,5,0,6},
//                {M,M,4,6,0}
//        };
//
//
//        int[][] weight2 = {
//                {0,10,M,30,100},
//                {M,0,50,M,M},
//                {M,M,0,M,10},
//                {M,M,20,0,60},
//                {M,M,M,M,0}
//        };
//        int start=0;
//        int[] shortPath = Dijsktra(weight2,start);
//
//        for(int i = 0;i < shortPath.length;i++)
//            System.out.println("从"+start+"出发到"+i+"的最短距离为："+shortPath[i]);
        int s = lengthOfLongestSubstring("aabbccdd");
        System.out.println(s);

    }
    public static int lengthOfLongestSubstring(String s) {
        if(s.length() == 0) return 0;

        int left = 0, res = 0;
        HashMap<Character, Integer> map = new HashMap<>();
        for(int i = 0; i < s.length(); i++) {
            if(map.containsKey(s.charAt(i))) left = Math.max(left, map.get(s.charAt(i))+1);

            map.put(s.charAt(i), i);
            res  = Math.max(res, i-left+1);
        }
        return res;
    }

    public static int[] Dijsktra(int[][] weight,int start){
        //接受一个有向图的权重矩阵，和一个起点编号start（从0编号，顶点存在数组中）
        //返回一个int[] 数组，表示从start到它的最短路径长度
        int n = weight.length;        //顶点个数
        int[] shortPath = new int[n];    //存放从start到其他各点的最短路径
        String[] path=new String[n]; //存放从start到其他各点的最短路径的字符串表示
        for(int i=0;i<n;i++)
            path[i]=new String(start+"-->"+i);
        int[] visited = new int[n];   //标记当前该顶点的最短路径是否已经求出,1表示已求出

        //初始化，第一个顶点求出
        shortPath[start] = 0;
        visited[start] = 1;

        for(int count = 1;count <= n - 1;count++)  //要加入n-1个顶点
        {

            int k = -1;    //选出一个距离初始顶点start最近的未标记顶点
            int dmin = Integer.MAX_VALUE;
            for(int i = 0;i < n;i++)
            {
                if(visited[i] == 0 && weight[start][i] < dmin)
                {
                    dmin = weight[start][i];

                    k = i;
                }

            }
            System.out.println("k="+k);

            //将新选出的顶点标记为已求出最短路径，且到start的最短路径就是dmin
            shortPath[k] = dmin;

            visited[k] = 1;

            //以k为中间点，修正从start到未访问各点的距离
            for(int i = 0;i < n;i++)
            {                 // System.out.println("k="+k);
                if(visited[i] == 0 && weight[start][k] + weight[k][i] < weight[start][i]){
                    weight[start][i] = weight[start][k] + weight[k][i];

                    path[i]=path[k]+"-->"+i;

                }

            }

        }
        for(int i=0;i<n;i++)
            System.out.println("从"+start+"出发到"+i+"的最短路径为："+path[i]);
        System.out.println("=====================================");

        return shortPath;
    }
}
