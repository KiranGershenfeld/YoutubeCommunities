# YouTube Communities
This project aims to visualize the YouTube creator ecosystem in a way that feels intuitive to users and creators.
 
The atlas has the top 5700 YouTube channels by subscriber count and links them together by how commenter overlap. The list of YouTube channels was provided by [SocialBlade](https://socialblade.com/) - Thanks!. 

All child targeted YouTube channels are not included as the commenters are disabled. 
 
Results, methodoloy, and tools used in this project are below. 
 
 ## Results
The atlas is too large to be very useful as an image so I have created an interactive version that is available at https://youtubeatlas.com. I am not a professional web developer so it may have some perfomance issues rendering such a massive graph. If the web version is not working for you here is an image:

 ![Atlas](https://imgur.com/4LsCNkj.png)
 
 ## Methodology and Process
 I recieved a list of the top YouTube channels by subscriber count courtesy of SocialBlade (Thanks!). 
 
Each of these channels was scraped for 20,000 commenters from their most recent videos. YouTube has relatively strict API quotas so this took place over the course of several months, about from August-October 2021. 
 
When I had enough data I computed the overlaps of commenters between each channel. This information was then transformed into a network graph in the graph visualization software  Gephi. I ran a modularity analysis on the graph to detect communities and colored the nodes accordingly. The graph layout was done using the Fruchterman-Reingold force-directed layout algorithm. 

I used the SimgaJS library to create a web visualization of the graph exported from Gephi. I had to do a ton of tinkering with SigmaJS to get it to work for what I needed to but I think its landed in a usable place. 
 
 ## Tools
 The following tools were used to complete this project:
 * Python
   - Pandas
   - Jupyter Notebooks
 * AWS:
   - S3 (Both for storage and web hosting)
   - EC2
   - CloudWatch
   - CloudFront
   - Route 53
   - Secrets Manager
 * Gephi
 * SimgaJS
 * Many many Notepad files
 
This project is in no way affiliated with YouTube and is simply a passion project by a curious student. 

Check out my other social media mapping project of [Twitch](https://github.com/KiranGershenfeld/VisualizingTwitchCommunities)!
