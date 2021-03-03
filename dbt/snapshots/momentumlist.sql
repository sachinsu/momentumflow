{% snapshot momentumstocks_snapshot %}


select * from {{ ref('weeklylist') }}

{% endsnapshot %}