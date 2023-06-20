<?php

namespace App\Repositories\Admin\Report;
use App\Repositories\Admin\Report\ReportInterface as ReportInterface;
use App\Models\EmployerJob; 
class ReportRepository implements ReportInterface
{

    /**
     * Create a new controller instance.
     *
     * @return void
     */
    function __construct() {
    }

    /**
     * Get Country jobs report
     * GroupBy Date AND Country/City Based on filter
     * Filter by JobSource, Created By, Country and Date-range
     *
     * @return array
     */
    public function getCountryJobsReport($input){
        $from_date =  new \MongoDB\BSON\UTCDateTime(Carbon::parse($input['start'])->startOfDay());
        $to_date =  new \MongoDB\BSON\UTCDateTime(Carbon::parse($input['end'])->endOfDay());
        $_match['posted_date'] = ['$gte' => $from_date, '$lte' => $to_date];
        
		$report_type = "Country";
        $report_typeId = "country_name";
        $model = "\App\Models\EmployerJob";
        
		if(isset($input['country_id']) && $input['country_id'] !=""){
            $_match['country_id'] = intval($input['country_id']);
            $report_type = "City";
            $report_typeId = "city_name";
        }
        if(isset($input['filter_category']) && $input['filter_category'] =="data-entry"){
            $_match['job_source'] = 1;
            $model = "\App\Models\Job";
        }else if(isset($input['filter_category']) && $input['filter_category'] =="direct"){
            $_match['job_source'] = 1;
        }else{
            
        }
        if(isset($input['filter_category']) && $input['filter_category'] =="ats" && isset($input['job_source']) && $input['job_source'] !=""){
            $_match['job_source'] = intval($input['job_source']);
        }
        if(isset($input['filter_category']) && $input['filter_category'] =="data-entry" && isset($input['posted_by']) && $input['posted_by'] !=""){
            $_match['created_by_id'] = intval($input['posted_by']);
        }

        $finaldata =  $model::raw(function($collection)use($_match,$report_typeId)
        {
            return $collection->aggregate([
                [
                    '$match' => $_match
                ],
                [
                    '$sort'=> [
                        'posted_date'=> -1
                    ]
                ],
                [
                    '$group' => [
                        '_id' => [
                            'day'    => ['$dayOfMonth' => ['date'=>'$posted_date','timezone'=>'Asia/Dubai']],
                            'month'  => ['$month' => ['date'=>'$posted_date','timezone'=>'Asia/Dubai']],
                            'year'   => ['$year' => ['date'=>'$posted_date','timezone'=>'Asia/Dubai']],
                            $report_typeId => '$'.$report_typeId
                        ],
                        'job_count' => [
                            '$sum' => 1
                        ]                       
                    ]
                ],
                [
                    '$sort'=> [
                        "_id.year"=> 1,
                        "_id.month"=> 1,
                        "_id.day"=> 1,
                    ]
                ],
            ]);
        });
		
        return $finaldata;
    }

    /**
     * Get ATS jobs report
     * GroupBy Date AND JobSource based on filter
     * Filter Country and Date-range
     *
     * @return array
     */
    public function getATSJobsReport($input){
        
		$from_date =  new \MongoDB\BSON\UTCDateTime(Carbon::parse($input['start'])->startOfDay());
        $to_date =  new \MongoDB\BSON\UTCDateTime(Carbon::parse($input['end'])->endOfDay());
		$_match['posted_date'] = ['$gte' => $from_date, '$lte' => $to_date];
		
        if(isset($input['country_id']) && $input['country_id'] !=""){
            $_match['country_id'] = intval($input['country_id']);
        }
        $finaldata =  EmployerJob::raw(function($collection)use($_match)
        {
            return $collection->aggregate([
                [
                    '$match' => $_match
                ],
                [
                    '$sort'=> [
                        'posted_date'=> -1
                    ]
                ],
                [
                    '$group' => [
                        '_id' => [
                            'day'    => ['$dayOfMonth' => ['date'=>'$posted_date','timezone'=>'Asia/Dubai']],
                            'month'  => ['$month' => ['date'=>'$posted_date','timezone'=>'Asia/Dubai']],
                            'year'   => ['$year' => ['date'=>'$posted_date','timezone'=>'Asia/Dubai']],
                            'job_source' => '$job_source'
                        ],
                        'job_count' => [
                            '$sum' => 1
                        ]                       
                    ]
                ],
                [
                    '$sort'=> [
                        "_id.year"=> 1,
                        "_id.month"=> 1,
                        "_id.day"=> 1,
                    ]
                ],
            ]);
        });
        return $finaldata;
    }

    public function getEmployerConversionReportData($input)
    {
        if (isset($input['start']) && isset($input['end'])) {
            $start = $input['start'];
            $end = $input['end'];
        } else {
            $start = Carbon::now()->format('Y-m-d');
            $end = Carbon::now()->format('Y-m-d');
        }
        $from_date =  new \MongoDB\BSON\UTCDateTime(Carbon::parse($start)->startOfDay());
        $to_date =  new \MongoDB\BSON\UTCDateTime(Carbon::parse($end)->endOfDay());

        $country = $this->country->where('is_deleted', 0)->pluck('name', 'id');

        if (isset($input['registered_date']) && $input['registered_date'] != "") {
            $group['day']   = ['$dayOfMonth' => ['date' => '$registered_date', 'timezone' => 'Asia/Dubai']];
            $group['month'] = ['$month' => ['date' => '$registered_date', 'timezone' => 'Asia/Dubai']];
            $group['year']  = ['$dayOfYear' => ['date' => '$registered_date', 'timezone' => 'Asia/Dubai']];
            $colgroup['registered_date'] = ['$first' => '$registered_date'];
        }
        if (isset($input['utm_source']) && $input['utm_source'] != "") {
            $group['source']   = '$utm_source';
            $colgroup['source']   = ['$first' => '$utm_source'];
        }
        if (isset($input['utm_medium']) && $input['utm_medium'] != "") {
            $group['utm_medium']   = '$utm_medium';
            $colgroup['utm_medium']   = ['$first' => '$utm_medium'];
        }
        if (isset($input['utm_campaign']) && $input['utm_campaign'] != "") {
            $group['utm_campaign']   = '$utm_campaign';
            $colgroup['utm_campaign']   = ['$first' => '$utm_campaign'];
        }
        if (isset($input['country_id']) && $input['country_id'] != "") {
            $group['country_id']   = '$country_id';
            $colgroup['country_id']   = ['$first' => '$country_id'];
        }
        $_group = array_merge(['_id' => $group, 'total' => ['$sum' => 1]], $colgroup);
        $_match['profile_complete'] = 1;
        $_match['utm_source'] = ['$exists' => true, '$ne' => ''];

        if (isset($input['utm_source']) && $input['utm_source'] != "" && isset($input['filter_utm_source']) && $input['filter_utm_source'] != "") {
            $_match['utm_source'] = $input['filter_utm_source'];
        }
        if (isset($input['utm_medium']) && $input['utm_medium'] != "" && isset($input['filter_utm_medium']) && $input['filter_utm_medium'] != "") {
            $_match['utm_medium'] = $input['filter_utm_medium'];
        }
        if (isset($input['utm_campaign']) && $input['utm_campaign'] != "" && isset($input['filter_utm_campaign']) && $input['filter_utm_campaign'] != "") {
            $_match['utm_campaign'] = $input['filter_utm_campaign'];
        }
        if (isset($input['country_id']) && $input['country_id'] != "" && isset($input['filter_country_id']) && $input['filter_country_id'] != "") {
            $_match['country_id'] = intval($input['filter_country_id']);
        }
        if (isset($input['registered_date']) && $input['registered_date'] != "") {
            $_match['registered_date'] = ['$gte' => $from_date, '$lte' => $to_date];
        }

        $selectsource =  MasterEmployer::where('utm_campaigncc', 'jig')->raw(function ($collection) use ($_match, $_group) {
            return $collection->aggregate([
                [
                    '$match' => $_match
                ],
                [
                    '$group' => $_group
                ]
            ]);
        });

        return  $selectsource->toArray();
    }
}
