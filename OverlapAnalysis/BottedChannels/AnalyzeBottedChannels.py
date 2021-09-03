import pickle as pkl
import pandas as pd

def main():
    with open('ChannelCommenterMap.pkl', 'rb') as handle:
        channel_commenter_map = pkl.load(handle)

    df = pd.DataFrame().from_dict(channel_commenter_map, orient='index', columns=['Commenter Count'])
    df['Channel Username'] = df.index
    print(df.head())

    sb_df = pd.read_csv('../Top20kYoutubeChannels.csv')
    print(sb_df.head())

    df = df.sort_values('Commenter Count')
    df['Display Name'] = df.apply(lambda x: (sb_df[sb_df['username'] == x['Channel Username']].iloc[0]['displayname']), axis=1)


    df.to_csv('ChannelCommenterCSV.csv')

if __name__ == '__main__':
    main()